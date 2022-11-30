import bson

### 0. Return all the entries in accounts collection
def query0(db):
    return db['accounts'].find({})

### 1. Find all information for the customer with username 'fmiller'
def query1(db):
    return db['customers'].find({"username": 'fmiller'})

### 2. For all customers with first name 'Natalie', return their username, name, and address
### Use 'regex' functionality to do the matching
def query2(db):
    return db['customers'].find(
        {"name": {"$regex": "Natalie"}}, 
        {"username": 1, "name": 1, "address": 1})

### 3. Find all accounts with a 'products' array containing 'Commodity' -- return the '_id' and 'account_id'
def query3(db):
    return db['accounts'].find({"products": "Commodity"}, {"account_id": 1})

### 4. Find all accounts with either limit <= 9000 or products array exactly ["Commodity", "InvestmentStock"] in that order -- return the entire accounts information
### Use "$or" to do a disjunction
def query4(db):
    return db['accounts'].find({"$or": 
        [
            {"limit": {"$lte": 9000}}, 
            {"products": ["Commodity", "InvestmentStock"]}
        ]
    })

### 5. Find all accounts with limit <= 9000 AND products array exactly ["Commodity", "InvestmentStock"] in that order -- return the entire accounts information
def query5(db):
    return db['accounts'].find({"$and": 
        [
            {"limit": {"$lte": 9000}},
            {"products": ["Commodity", "InvestmentStock"]}
        ]
    })

### 6. Find all accounts where the second entry in the products array is 'Brokerage' -- return the entire accounts information
def query6(db):
    return db['accounts'].find({"products.1": "Brokerage"})

### 7. On the customers collection, use aggregation and grouping to find the number of customers born in each month
### The output will contain documents like: {'_id': 7, 'totalCount': 42} 
### Use '$month' function to operate on the dates, and use '$sum' aggregate to do the counting
### https://database.guide/mongodb-month/
def query7(db):
    pipeline = [
        {"$project": {"birthMonth": {"$month": "$birthdate"}}},
        {"$group": 
            {"_id": "$birthMonth", "totalCount": {"$sum": 1}}
        }
    ]
    return db['customers'].aggregate(pipeline)

### 8. Modify the above query to only count the customers whose name starts with any letter between 'A' and 'G' (inclusive). 
### The output will contain documents like: {'_id': 2, 'totalCount': 17}
###
### Use '$match' along with '$group' as above.
def query8(db):
    pipeline = [
        {"$project": {
            "birthdate": 1,
            "firstLetter": {"$substrCP": ["$name", 0, 1]}}},
        {"$match": {"firstLetter": {"$regex": "^[A-G]"}}},
        {"$project": {
            "firstLetter": 1,
            "birthMonth": {"$month": "$birthdate"}}},
        {"$group": {"_id": "$birthMonth", "totalCount": {"$sum": 1}}}
    ]
    return db['customers'].aggregate(pipeline)

### 9. In the 'transactions' collection, all transactions are inside a single array, making it difficult to operate on them. 
### However, we can use 'unwind' to create a separate document for each of the transactions. 
### The query below shows this for a single account to see how this work: 
###             [{'$match': {'account_id': 443178}}, {'$unwind': '$transactions'}]
###
### Use 'unwind' as above to create a list of documents where each document is simply: _id, account_id, symbol (inside the transaction), transaction_code ("sell" or "buy"), and amount (sold/bought)
### Restrict this to accounts with fewer than 10 transactions
###
### One of the outputs:
### {'_id': ObjectId('5ca4bbc1a2dd94ee58161cd5'), 'account_id': 463155, 'transactions': {'amount': 6691, 'symbol': 'amd', 'transaction_code': 'buy'}}
###
def query9(db):
    pipeline = [
        {"$match": {"transaction_count": {"$lt": 10}}},
        {"$unwind": "$transactions"},
        {"$project": {
            "_id": 1,
            "account_id": 1,
            "transactions.amount": 1,
            "transactions.symbol": 1,
            "transactions.transaction_code": 1}}
    ]
    return db['transactions'].aggregate(pipeline)

### 10. Use the result of the above query to compute the total number shares sold or bought for each symbol across the entire collection of accounts
### However, DO NOT restrict it to only accounts with < 10 transactions.
### Use '$sort' to sort the outputs in descending order by the total count of shares
### First few outputs look like this:
###       {'_id': 'adbe', 'totalCount': 27463715}
###       {'_id': 'ebay', 'totalCount': 27232371}
###       {'_id': 'crm', 'totalCount': 27099929}
###       {'_id': 'goog', 'totalCount': 27029894}
###       {'_id': 'nvda', 'totalCount': 26108705}
def query10(db):
    pipeline = [
        {"$unwind": "$transactions"},
        {"$project": {
            "_id": 0,
            "amount": "$transactions.amount",
            "symbol": "$transactions.symbol"}},
        {"$group": {
            "_id": "$symbol",
            "totalCount": {"$sum": "$amount"}
        }},
        {"$sort": {"totalCount": -1}}        
    ]
    return db['transactions'].aggregate(pipeline)

### 11. Use $lookup to do a "join" between customers and accounts to find, for each customer the number of accounts they have with 'InvestmentFund' as a product (i.e., the number of their accounts where 'InvestmentFund' is in the 'products' array).
### Sort the final output by 'username' in the ascending order
### First few outputs
###        {'_id': 'abrown', 'totalCount': 1}
###        {'_id': 'alexandra72', 'totalCount': 2}
###        {'_id': 'alexsanders', 'totalCount': 2}
###        {'_id': 'allenhubbard', 'totalCount': 2}
###        {'_id': 'alvarezdavid', 'totalCount': 3}
def query11(db):
    pipeline = [
        {"$unwind": "$accounts"},
        {"$project": {
            "_id": 0,
            "username": 1,
            "account": "$accounts"
        }},
        {"$lookup": {
            "from": "accounts",
            "localField": "account",
            "foreignField": "account_id",
            "as": "account_info"
        }},
        {"$unwind": "$account_info"},
        {"$project": {
            "account": 1,
            "username": 1,
            "products": "$account_info.products"
        }},
        {"$match": {"products": "InvestmentFund"}},
        {"$group": {
            "_id": "$username",
            "totalCount": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    return db['customers'].aggregate(pipeline)

### 12. We want to find all accounts that have exactly 3 products and <= 10 transactions associated with them.
### Use '$lookup' and '$group' to do so by joining accounts and transactions. This would be a multi-stage pipeline, possibly with multiple groups and matches.
###
### Output all the information for the matching accounts as shown below.
### {'_id': ObjectId('5ca4bbc7a2dd94ee58162576'),
### 'account_id': 154391,
### 'products': ['Brokerage', 'Commodity', 'InvestmentStock'],
### 'transaction_count': 4}
###
### Use unwind and addFields to add the top-level field 'transaction_count' to accounts
### Sort the final output by account_id
def query12(db):
    pipeline = [
        {"$project": {
            "_id": 1,
            "account_id": 1,
            "products": 1
        }},
        {"$lookup": {
            "from": "transactions",
            "localField": "account_id",
            "foreignField": "account_id",
            "as": "transaction_info"
        }},
        {"$project": {
            "_id": 1,
            "account_id": 1,
            "products": 1,
            "transaction_count": "$transaction_info.transaction_count"
        }},
        {"$unwind": "$transaction_count"},
        {"$match": {"$and":
            [
                {"transaction_count": {"$lte": 10}},
                {"products": {"$size": 3}}
            ]
        }},
        {"$sort": {"account_id": 1}}
    ]
    return db['accounts'].aggregate(pipeline)

### 13. Using an aggregation and '$addToSet' and '$first', write a query whose output has a multi-attribute _id comprising of account_id and year, with two aggregates: 
### a set listing all the symbols that were traded by that account in that year, and 
### the first symbol traded in that year for that account_id
### Sort the final result by "_id" (a combination of account_id and year)
### First few results look like:
### {'_id': {'account_id': 50948, 'year': 1995}, 'traded_symbols': ['aapl'], 'first_symbol': 'aapl'}, 
### {'_id': {'account_id': 50948, 'year': 2004}, 'traded_symbols': ['csco'], 'first_symbol': 'csco'},
### {'_id': {'account_id': 50948, 'year': 2006}, 'traded_symbols': ['aapl'], 'first_symbol': 'aapl'},
def query13(db):
    pipeline = [
        {"$project": {
            "_id": 0,
            "account_id": 1,
            "transactions": 1
        }},
        {"$unwind": "$transactions"},
        {"$project": {
            "account_id": 1,
            "transaction_year": {"$year": "$transactions.date"},
            "transaction_symbol": "$transactions.symbol"
        }},
        {"$group": {
            "_id": {"account_id": "$account_id", "year": "$transaction_year"},
            "traded_symbols": {"$addToSet": "$transaction_symbol"}
        }},
        {"$project": {
            "_id": 1,
            "traded_symbols": {
                "$sortArray": {"input": "$traded_symbols", "sortBy": 1}
            }
        }},
        {"$project": {
            "_id": 1,
            "traded_symbols": 1,
            "first_symbol": {"$first": "$traded_symbols"}
        }},
        {"$sort": {"_id": 1}}        
    ]
    return db['transactions'].aggregate(pipeline)

### 14. Create an output that associates each "product" (found in 'accounts') and each "symbol" (found in 'transactions') with the total volume
### of shares bought/sold (i.e., 'amount' in each transaction).
### Restrict the computation to accounts with exactly 3 transactions (takes too long too run otherwise).
### Sort the final result by "_id".
### First few results look like:
### {'_id': {'product': 'Brokerage', 'symbol': 'aapl'}, 'total_amount': 18081},
### {'_id': {'product': 'Brokerage', 'symbol': 'adbe'}, 'total_amount': 2328},
### {'_id': {'product': 'Brokerage', 'symbol': 'amzn'}, 'total_amount': 9802},
### {'_id': {'product': 'Brokerage', 'symbol': 'bb'}, 'total_amount': 1415},
def query14(db):
    pipeline = [
        {"$match": {"transaction_count": {"$eq": 3}}},
        {"$unwind": "$transactions"},
        {"$project": {
            "_id": 0,
            "account_id": 1,
            "amount": "$transactions.amount",
            "symbol": "$transactions.symbol",
        }},
        {"$lookup": {
            "from": "accounts",
            "localField": "account_id",
            "foreignField": "account_id",
            "as": "acct_info"
        }},
        {"$project": {
            "account_id": 1,
            "amount": 1,
            "symbol": 1,
            "products": "$acct_info.products",
        }},
        {"$unwind": "$products"},
        {"$unwind": "$products"},
        {"$group": {
            "_id": {"product": "$products", "symbol": "$symbol"},
            "total_amount": {"$sum": "$amount"}
        }},
        {"$sort": {"_id": 1}}
    ]
    return db['transactions'].aggregate(pipeline)

### 15. Let's create a copy of the accounts collection using: db['accounts'].aggregates([ {"$out": "accounts_copy"} ])
### Write the code to insert 10 more documents into the collection with data:
###      oid = 5ca4bbc7a2dd94ee58162a61, ..., 5ca4bbc7a2dd94ee58162a6a
###      account_id = 11, ..., 20
###      limit = 10000 (for all)
###      products = ["Brokerage", "InvestmentStock"] for all
###  Make sure you match the data types present in the dataset right now. You may need to use bson.objectid.ObjectId
def query15(db):
    db['accounts'].aggregate([ {"$out": "accounts_copy"} ])

    db['accounts_copy'].insert_many([
        {
            "_id": bson.ObjectId("5ca4bbc7a2dd94ee58162a61"),
            "account_id": 11,
            "limit": 10000,
            "products": ["Brokerage", "InvestmentStock"]
        },
        {
            "_id": bson.ObjectId("5ca4bbc7a2dd94ee58162a62"),
            "account_id": 12,
            "limit": 10000,
            "products": ["Brokerage", "InvestmentStock"]
        },
        {
            "_id": bson.ObjectId("5ca4bbc7a2dd94ee58162a63"),
            "account_id": 13,
            "limit": 10000,
            "products": ["Brokerage", "InvestmentStock"]
        },
        {
            "_id": bson.ObjectId("5ca4bbc7a2dd94ee58162a64"),
            "account_id": 14,
            "limit": 10000,
            "products": ["Brokerage", "InvestmentStock"]
        },
        {
            "_id": bson.ObjectId("5ca4bbc7a2dd94ee58162a65"),
            "account_id": 15,
            "limit": 10000,
            "products": ["Brokerage", "InvestmentStock"]
        },
        {
            "_id": bson.ObjectId("5ca4bbc7a2dd94ee58162a66"),
            "account_id": 16,
            "limit": 10000,
            "products": ["Brokerage", "InvestmentStock"]
        },
        {
            "_id": bson.ObjectId("5ca4bbc7a2dd94ee58162a67"),
            "account_id": 17,
            "limit": 10000,
            "products": ["Brokerage", "InvestmentStock"]
        },
        {
            "_id": bson.ObjectId("5ca4bbc7a2dd94ee58162a68"),
            "account_id": 18,
            "limit": 10000,
            "products": ["Brokerage", "InvestmentStock"]
        },
        {
            "_id": bson.ObjectId("5ca4bbc7a2dd94ee58162a69"),
            "account_id": 19,
            "limit": 10000,
            "products": ["Brokerage", "InvestmentStock"]
        },
        {
            "_id": bson.ObjectId("5ca4bbc7a2dd94ee58162a6a"),
            "account_id": 20,
            "limit": 10000,
            "products": ["Brokerage", "InvestmentStock"]
        }
    ])

    return db['accounts_copy'].find( {"account_id": {"$gte": 11, "$lte": 20}} )

### 16. Add a new "sub-document" to the "customers_copy" table (a copy of customers) of the form:
###        {"summary": {"num_accounts": <the number of accounts for the customer>}}
###
### Note that, the simple update_many doesn't work here -- you have to use an aggregation pipeline (with just one stage)
### https://www.mongodb.com/docs/manual/reference/method/db.collection.updateMany/#std-label-updateMany-behavior-aggregation-pipeline
###
### For the first customer (fmiller), the data would look like:
###        {"summary": {"num_accounts": 6}}
### For the second customer (valenciajennifer), the data would look like:
###        {"summary": {"num_accounts": 1}}
def query16(db):
    db['customers'].aggregate([ {"$out": "customers_copy"} ])

    db['customers_copy'].update_many(
        {}, 
        [
            {"$set":{"summary": {"num_accounts": {"$size": "$accounts"}}}}
        ]
    )

    return db['customers_copy'].find({}, {"summary": 1})

### 17. Here we will use the '$merge' stage to update a document with more complex information than possible with the basic update_many command.
### Specifically, we will add a list of "products" into the "customers" collection -- i.e., for each customer, we will find the list of products for 
### all of their accounts, and add it as a array (without duplication) into the customers collection
### You may have to use multiple unwinds to get this result, along with an "addToSet" to create a set of all products without duplication
### '$merge' must be the final stage, and should be used as: 
###            {'$merge': { 'into': "customers_copy2", 'on': "_id", 'whenMatched': "merge", 'whenNotMatched': "insert" }}
###  For more details on merge, see: https://www.mongodb.com/docs/manual/reference/operator/aggregation/merge/#mongodb-pipeline-pipe.-merge
###
### The document with username 'fmiller' would look like (only the last field is different):
### {'_id': ObjectId('5ca4bbcea2dd94ee58162a68'),
###   'username': 'fmiller',
###   'name': 'Elizabeth Ray',
###   'address': '9286 Bethany Glens\nVasqueztown, CO 22939',
###   'birthdate': datetime.datetime(1977, 3, 2, 2, 20, 31),
###   'email': 'arroyocolton@gmail.com',
###   'active': True,
###   'accounts': [371138, 324287, 276528, 332179, 422649, 387979],
###   'tier_and_details': {'0df078f33aa74a2e9696e0520c1a828a': {'tier': 'Bronze', 'id': '0df078f33aa74a2e9696e0520c1a828a', 'active': True, 'benefits': ['sports tickets']}, '699456451cc24f028d2aa99d7534c219': {'tier': 'Bronze', 'benefits': ['24 hour dedicated line', 'concierge services'], 'active': True, 'id': '699456451cc24f028d2aa99d7534c219'}},
###   'allProducts': ['InvestmentStock', 'Brokerage', 'CurrencyService', 'Commodity', 'Derivatives', 'InvestmentFund']}
###
def query17(db):
    db['customers'].aggregate([ {"$out": "customers_copy2"} ])

    db['customers_copy2'].aggregate([
        {'$unwind': "$accounts"},
        {'$lookup': {
            "from": "accounts",
            "localField": "accounts",
            "foreignField": "account_id",
            "as": "account_info"
        }},
        {'$project': {
            "accounts": 1,
            "allProducts": "$account_info.products"
        }},
        {'$unwind': "$allProducts"},
        {'$unwind': "$allProducts"},
        {'$group': {
            "_id": "$_id",
            "accounts": {"$addToSet": "$accounts"},
            "allProducts": {"$addToSet": "$allProducts"}
        }},
        {'$merge': { 'into': "customers_copy2", 'on': "_id", 'whenMatched': "merge", 'whenNotMatched': "insert" }}
    ])

    #return db['customers_copy2'].find({})
    return db['customers_copy2'].find({}, {'allProducts': 1})
