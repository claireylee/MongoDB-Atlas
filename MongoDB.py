from pymongo import MongoClient
connection_string = 'mongodb+srv://ngt9dz:Cookies1@cluster1.3osh6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster1'
client = MongoClient(connection_string)
db = client['sample_mflix']
collection = db['movies']
print(collection.find_one())

#EXERCISE 1
#1. find the first movie with the genre "Action"
action_movie = collection.find_one({"genres": "Actions"})
print("First Action movie:", action_movie)

#2. find all movies released after the year 2000 (limit to 5)
movies_after_2000 = collection.find({"year": {"$gt": 2000}}).limit(5)
print("Movies released after 2000:")
for movie in movies_after_2000:
    print(movie)

#3. find all movies with IMDb rating greater than 8.5 (limit to 5)
high_rated_movies = collection.find({"imdb.rating": {"$gt": 8.5}}).limit(5)
print("Movies with IMDb rating > 8.5:")
for movie in high_rated_movies:
    print(movie)

#4. find all movies with genres containing both "Action" and "Adventure"
action_adventure_movies = collection.find({"genres":{"$all": ["Action", "Adventure"]}}).limit(5)
print("Action and Adventure movies:")
for movie in action_adventure_movies:
    print(movie)

#EXERCISE 2
# 1. find all movies where the genre is "Comedy" and sort by IMDb rating in descending order (limit to 5)
sorted_comedy_movies = collection.find({"genres": "Comedy"}).sort("imdb.rating", -1).limit(5)
print("Top 5 Comedy movies sorted by IMDb rating:")
for movie in sorted_comedy_movies:
    print(movie)

# 2. find all movies where the genre is "Drama" and sort by release year in ascending order (limit to 5)
sorted_drama_movies = collection.find({"genres": "Drama"}).sort("year", 1).limit(5)
print("Top 5 Drama movies sorted by release year:")
for movie in sorted_drama_movies:
    print(movie)

#EXERCISE 3
# 1. calculate the average IMDb rating for each genre (Return top 5 genres)
avg_rating_by_genre = collection.aggregate([
    {"$unwind": "$genres"},
    {"$group": {"_id": "$genres", "avg_rating": {"$avg": "$imdb.rating"}}},
    {"$sort": {"avg_rating": -1}},
    {"$limit": 5}
])
print("Top 5 genres by average IMDb rating:")
for genre in avg_rating_by_genre:
    print(genre)

# 2. find the top 5 directors by the average IMDb rating of their movies
top_directors = collection.aggregate([
    {"$group": {"_id": "$directors", "avg_rating": {"$avg": "$imdb.rating"}}},
    {"$sort": {"avg_rating": -1}},
    {"$limit": 5}
])
print("Top 5 directors by average IMDb rating:")
for director in top_directors:
    print(director)

# 3. calculate the total number of movies released in each year, sorted by year
movies_per_year = collection.aggregate([
    {"$group": {"_id": "$year", "total_movies": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
])
print("Total number of movies released per year:")
for year in movies_per_year:
    print(year)

#EXERCISE 4
# 1. update the IMDb rating of "The Godfather" to 9.5
collection.update_one({"title": "The Godfather"}, {"$set": {"imdb.rating": 9.5}})
print("Updated IMDb rating of The Godfather to 9.5")

# 2. update all Horror movies with IMDb rating set to 6.0 if it's currently null
collection.update_many({"genres": "Horror", "imdb.rating": {"$exists": False}}, {"$set": {"imdb.rating": 6.0}})
print("Set IMDb rating to 6.0 for all Horror movies with no rating")

# 3. delete all movies released before the year 1950
collection.delete_many({"year": {"$lt": 1950}})
print("Deleted all movies released before 1950")

#EXERCISE 5
# ensure the title field is indexed for text search
# word "love"
love_movies = collection.find({
    "$text": {"$search": "love"},
    "title": {"$regex": "love", "$options": "i"}  # Ensure the word 'love' is in the title
})
for movie in love_movies:
    print(movie)

# 2. text search for movies with 'war' in title or plot, sorted by IMDb rating (limit to 5)war_movies = collection.find({
#     "$text": {"$search": "war"}
# }).sort("imdb.rating", -1).limit(5)
#
# for movie in war_movies:
#     print(movie)

#EXERCISE 6
# 1. find all Action movies with IMDb rating greater than 8, sorted by release year in descending order
action_high_rated_movies = collection.find({"genres": "Action", "imdb.rating": {"$gt": 8}}).sort("year", -1)
print("Action movies with IMDb rating > 8, sorted by release year:")
for movie in action_high_rated_movies:
    print(movie)

# 2. find top 3 movies directed by Christopher Nolan with IMDb rating greater than 8, sorted by rating
nolan_movies = collection.find({"directors": "Christopher Nolan", "imdb.rating": {"$gt": 8}}).sort("imdb.rating", -1).limit(3)
print("Top 3 Christopher Nolan movies with IMDb rating > 8:")
for movie in nolan_movies:
    print(movie)


