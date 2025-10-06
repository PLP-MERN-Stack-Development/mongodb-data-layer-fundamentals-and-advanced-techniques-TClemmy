// Import MongoDB client
const { MongoClient } = require('mongodb');

// Connection URI (replace with your MongoDB connection string if using Atlas)
const uri = 'mongodb+srv://clemmie_db_user:1240135tema@cluster0.alon9pn.mongodb.net/';
//const uri = 'mongodb://localhost:27017';

// Database and collection names
const dbName = 'plp_bookstore';
const collectionName = 'books';

// Sample book data
const books = [
  {
    title: 'To Kill a Mockingbird',
    author: 'Harper Lee',
    genre: 'Fiction',
    published_year: 1960,
    price: 12.99,
    in_stock: true,
    pages: 336,
    publisher: 'J. B. Lippincott & Co.'
  },
  {
    title: '1984',
    author: 'George Orwell',
    genre: 'Dystopian',
    published_year: 1949,
    price: 10.99,
    in_stock: true,
    pages: 328,
    publisher: 'Secker & Warburg'
  },
  {
    title: 'The Great Gatsby',
    author: 'F. Scott Fitzgerald',
    genre: 'Fiction',
    published_year: 1925,
    price: 9.99,
    in_stock: true,
    pages: 180,
    publisher: 'Charles Scribner\'s Sons'
  },
  {
    title: 'Brave New World',
    author: 'Aldous Huxley',
    genre: 'Dystopian',
    published_year: 1932,
    price: 11.50,
    in_stock: false,
    pages: 311,
    publisher: 'Chatto & Windus'
  },
  {
    title: 'The Hobbit',
    author: 'J.R.R. Tolkien',
    genre: 'Fantasy',
    published_year: 1937,
    price: 14.99,
    in_stock: true,
    pages: 310,
    publisher: 'George Allen & Unwin'
  },
  {
    title: 'The Catcher in the Rye',
    author: 'J.D. Salinger',
    genre: 'Fiction',
    published_year: 1951,
    price: 8.99,
    in_stock: true,
    pages: 224,
    publisher: 'Little, Brown and Company'
  },
  {
    title: 'Pride and Prejudice',
    author: 'Jane Austen',
    genre: 'Romance',
    published_year: 1813,
    price: 7.99,
    in_stock: true,
    pages: 432,
    publisher: 'T. Egerton, Whitehall'
  },
  {
    title: 'The Lord of the Rings',
    author: 'J.R.R. Tolkien',
    genre: 'Fantasy',
    published_year: 1954,
    price: 19.99,
    in_stock: true,
    pages: 1178,
    publisher: 'Allen & Unwin'
  },
  {
    title: 'Animal Farm',
    author: 'George Orwell',
    genre: 'Political Satire',
    published_year: 1945,
    price: 8.50,
    in_stock: false,
    pages: 112,
    publisher: 'Secker & Warburg'
  },
  {
    title: 'The Alchemist',
    author: 'Paulo Coelho',
    genre: 'Fiction',
    published_year: 1988,
    price: 10.99,
    in_stock: true,
    pages: 197,
    publisher: 'HarperOne'
  },
  {
    title: 'Moby Dick',
    author: 'Herman Melville',
    genre: 'Adventure',
    published_year: 1851,
    price: 12.50,
    in_stock: false,
    pages: 635,
    publisher: 'Harper & Brothers'
  },
  {
    title: 'Wuthering Heights',
    author: 'Emily Brontë',
    genre: 'Gothic Fiction',
    published_year: 1847,
    price: 9.99,
    in_stock: true,
    pages: 342,
    publisher: 'Thomas Cautley Newby'
  }
];

// Function to insert books into MongoDB
async function insertBooks() {
  const client = new MongoClient(uri);

  try {
    // Connect to the MongoDB server
    await client.connect();
    console.log('Connected to MongoDB server');

    // Get database and collection
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    // Check if collection already has documents
    const count = await collection.countDocuments();
    if (count > 0) {
      console.log(`Collection already contains ${count} documents. Dropping collection...`);
      await collection.drop();
      console.log('Collection dropped successfully');
    }

    // Insert the books
    const result = await collection.insertMany(books);
    console.log(`${result.insertedCount} books were successfully inserted into the database`);

    // Display the inserted books
    console.log('\nInserted books:');
    const insertedBooks = await collection.find({}).toArray();
    insertedBooks.forEach((book, index) => {
      console.log(`${index + 1}. "${book.title}" by ${book.author} (${book.published_year})`);
    });

  } catch (err) {
    console.error('Error occurred:', err);
  } finally {
    // Close the connection
    await client.close();
    console.log('Connection closed');
  }
}

// Run the function
insertBooks().catch(console.error);

// Task 1: MongoDB setup, Complete
//DB - plp_bookstore, Collection - books

// Task 2: Basic CRUD
//find all books in specific genre: Fantasy
db.books.find({ genre: "Fantasy"})
//find books published after a certain year: 2000
db.books.find( {published_year: {$gt: 200}})
//Find books by a specific author: J.R.R. Tolkien
db.books.find({author: "J.R.R. Tolkien"})
//update the price of the specific book: The Hobbit
db.books.updateOne({title: "The Hobbit"}, {$set: {price: 19.95}})
//delete a book by its title - Animal Farm
db.books.deleteOne({title: "Animal Farm"})

// Task 3: Advanced Queries
//find books that are both in stock and published after 2010
db.books.find({published_year: {$gt: 2010}}, {in_stock: true})
//use projection to return only the title, author, and price fields
db.books.find({}, {title: 1, author: 1, price: 1})
//implement sorting to display books by price
db.books.find().sort({price: 1})//ascending order
db.books.find().sort({print: -1})//descending order
//use the limit and skip methods to implement pagination(5 per/page)
const pageNo = 1;
const pageSize = 5;
const skip = (pageNo - 1)*pageSize;
db.books.find().skip(skip).limit(pageSize);

// Task 4: Aggregation Pipeline
//pipeline to cal average price of books by genre
db.books.aggregate([
    {
        $group: {
            _id: "$genre", averagePrice: {$avg: "$price"}
        }
    }
])
//pipeline to find the author  with the most collection
db.books.aggregate([
    {
        $group: {
            _id: "$author", totalBooks: {$sum : 1}
        }
    },
    {
        $limit: 1
    }
])
//pipeline that groups the books by publication decade and counts them
db.books.aggregate([
    {
        $addFields: {
            decade: {
                $multiply: [
                    {$floor: {$divide: ["$published_year]", 10] } }, 10
                ]
            }
        }
    },
    {
        $group: {_id: "$decade", count: {$sum: 1}}
    }
])

// Task 5: Indexing
//an index on the title field
db.books.createIndex({title: 1})
//a compound index on author and published year
db.books.createIndex({author: 1, published_year: 1})
//the explain method to demonstrate the performance improvement
db.books.find({author: 1}).explain("executionstats")