from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['task']

# Aggregate total debit and credit amounts for each user
pipeline = [
    {
        '$group': {
            '_id': '$user_id',
            'totalDebit': {
                '$sum': {
                    '$cond': {
                        'if': {'$eq': ['$type', 'd']},
                        'then': '$amount',
                        'else': 0
                    }
                }
            },
            'totalCredit': {
                '$sum': {
                    '$cond': {
                        'if': {'$eq': ['$type', 'c']},
                        'then': '$amount',
                        'else': 0
                    }
                }
            }
        }
    },
    {
        "$skip": 0
    },
    {
        "$limit": 10
    },
    {
        '$project': {
            'totalAmount': {'$subtract': ['$totalCredit', '$totalDebit']}
        }
    },
    {
        '$out': 'tempTransactionTotals'
    }
]

# Execute aggregation pipeline
db.transaction.aggregate(pipeline)

# Update user balances based on the aggregated total amount
# for doc in db.tempTransactionTotals.find():
#     db.user.update_one(
#         {'id': doc['_id']},
#         {'$inc': {'balance': doc['totalAmount']}}
#     )

# Clean up temporary collection
# db.tempTransactionTotals.drop()
