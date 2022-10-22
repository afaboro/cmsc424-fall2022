import json
from flask import Flask, jsonify
from flask_restful import Api, Resource, reqparse
from flask_cors import CORS
import psycopg2

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
api = Api(app)
CORS(app)

class Post(Resource):
    def get(self, postid):
        #####################################################################################3
        #### Important -- This is the how the connection must be done for autograder to work
        ### But on your local machine, you may need to remove "host=..." part if this doesn't work
        #####################################################################################3
        conn = psycopg2.connect("host=127.0.0.1 dbname=stackexchange user=root password=root")
        cur = conn.cursor()

        cur.execute("select id, posttypeid, title, AcceptedAnswerID, creationdate from posts where id = %s" % (postid))
        ans = cur.fetchall()
        if len(ans) == 0:
            return "Post Not Found", 404
        else:
            ret = {"id": ans[0][0], "PostTypeID": ans[0][1], "Title": str(ans[0][2]), "AcceptedAnswerID": str(ans[0][3]), "CreationDate": str(ans[0][4])}
            return ret, 200


class Dashboard(Resource):
    # Return some sort of a summary of the data -- we will use the "name" attribute to decide which of the dashboards to return
    # 
    # Here the goal is to return the top 100 users using the reputation -- this will be returned as an array in increasing order of Rank
    # Use PostgreSQL default RANK function (that does sparse ranking), followed by a limit 100 to get the top 100 
    #
    # FORMAT: {"Top 100 Users by Reputation": [{"ID": "...", "DisplayName": "...", "Reputation": "...", "Rank": "..."}, {"ID": "...", "DisplayName": "...", "Reputation": "...", "Rank": "..."}, ]
    def get(self, name):
        conn = psycopg2.connect("host=127.0.0.1 dbname=stackexchange user=root password=root")
        cur = conn.cursor()

        if name == "top100users":
            rank = "select id, displayname, reputation, rank () over (order by reputation desc) rank from users order by rank asc limit 100"

            cur.execute(rank)
            ans = cur.fetchall()
            data = []
            for user in ans:
                data.append({'ID': user[0], 'DisplayName': user[1], 'Reputation': user[2], 'Rank': user[3]})
            
            return jsonify({'Top 100 Users by Reputation': data})
        else:
            return "Unknown Dashboard Name", 404

class User(Resource):
    # Return all the info about a specific user, including the titles of the user's posts as an array
    # The titles array must be sorted in the increasing order by the title.
    # Remove NULL titles if any
    # FORMAT: {"ID": "...", "DisplayName": "...", "CreationDate": "...", "Reputation": "...", "PostTitles": ["posttitle1", "posttitle2", ...]}
    def get(self, userid):
        # Add your code to construct "ret" using the format shown below
        # Post Titles must be sorted in alphabetically increasing order
        # CreationDate should be of the format: "2007-02-04" (this is what Python str() will give you)

        conn = psycopg2.connect("host=127.0.0.1 dbname=stackexchange user=root password=root")
        cur = conn.cursor()

        # Add your code to check if the userid is already present in the database
        find = "select id from users where id = (%s)"
        data = (userid, )
        cur.execute(find, data)
        ans = cur.fetchall()

        if len(ans) == 0:
            exists_user = False
        else: 
            exists_user = True

        if not exists_user:
            cur.close()
            return "User not found", 404
        else:
            part1 = "select id, displayname, creationdate, reputation from users where id = (%s)"
            part2 = "select title from posts where posts.owneruserid = (%s) and title is not null order by title asc"
            cur.execute(part1, data)
            user = cur.fetchone()
            cur.execute(part2, data)
            posts = cur.fetchall()
            posts = list(sum(posts, ()))
            ret = {"ID": user[0], "DisplayName": user[1], "CreationDate": str(user[2]), "Reputation": user[3], "PostTitles": posts}
            cur.close()
            return ret, 200

    # Add a new user into the database, using the information that's part of the POST request
    # We have provided the code to parse the POST payload
    # If the "id" is already present in the database, a FAILURE message should be returned
    def post(self, userid):
        parser = reqparse.RequestParser()
        parser.add_argument("reputation")
        parser.add_argument("creationdate")
        parser.add_argument("displayname")
        parser.add_argument("upvotes")
        parser.add_argument("downvotes")
        args = parser.parse_args()
        print("Data received for new user with id {}".format(userid))
        print(args)

        # Add your code to check if the userid is already present in the database
        conn = psycopg2.connect("host=127.0.0.1 dbname=stackexchange user=root password=root")
        cur = conn.cursor()
        
        find = "select id from users where id = (%s)"
        data = (userid, )
        cur.execute(find, data)
        ans = cur.fetchall()

        if len(ans) == 0:
            exists_user = False
        else: 
            exists_user = True

        if exists_user:
            return "FAILURE -- Userid must be unique", 201
        else:
            # Add your code to insert the new tuple into the database
            insert = "insert into users(id, reputation, creationdate, displayname, upvotes, downvotes, views) values (%s, %s, %s, %s, %s, %s, 0)"
            data = (userid, args['reputation'], args['creationdate'], args['displayname'], args['upvotes'], args['downvotes'])
            cur.execute(insert, data)
            conn.commit()
            return "SUCCESS", 201

    # Delete the user with the specific user id from the database
    def delete(self, userid):
        # Add your code to check if the userid is present in the database
        conn = psycopg2.connect("host=127.0.0.1 dbname=stackexchange user=root password=root")
        cur = conn.cursor()
        
        find = "select id from users where id = (%s)"
        data = (userid, )
        cur.execute(find, data)
        ans = cur.fetchall()

        if len(ans) == 0:
            exists_user = False
        else: 
            exists_user = True

        if exists_user:
            # Add your code to delete the user from the user table
            # If there are corresponding entries in "badges" table for that userid, those should be deleted
            # For posts, comments, votes, set the appropriate userid fields to -1 (since that content should not be deleted)
            
            replace_post_owner = "update posts set owneruserid = -1 where owneruserid = (%s)"
            cur.execute(replace_post_owner, data)
            conn.commit()

            replace_post_editor = "update posts set lasteditoruserid = -1 where lasteditoruserid = (%s)"
            cur.execute(replace_post_editor, data)
            conn.commit()

            replace_comment = "update comments set userid = -1 where userid = (%s)"
            cur.execute(replace_comment, data)
            conn.commit()

            replace_votes = "update votes set userid = -1 where userid = (%s)"
            cur.execute(replace_votes, data)
            conn.commit()
            
            remove_badges = "delete from badges where userid = (%s)"
            cur.execute(remove_badges, data)
            conn.commit()

            delete_user = "delete from users where id = (%s)"
            cur.execute(delete_user, data)
            conn.commit()            
            return "SUCCESS", 201
        else:
            return "FAILURE -- Unknown Userid", 404
      
api.add_resource(User, "/user/<int:userid>")
api.add_resource(Post, "/post/<int:postid>")
api.add_resource(Dashboard, "/dashboard/<string:name>")

app.run(debug=True, host="0.0.0.0", port=5000)
