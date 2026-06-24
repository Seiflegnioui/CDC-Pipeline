// ── MongoDB Replica Set Initialization ──────────────────────────────────
// This script initializes the replica set, creates the database and
// collections, and seeds each collection with sample documents.

// Step 1: Initiate replica set "rs0"
print("=== Initiating Replica Set rs0 ===");
try {
  rs.initiate({
    _id: "rs0",
    members: [{ _id: 0, host: "mongodb:27017" }]
  });
} catch (e) {
  print("Replica set already initiated or error: " + e.message);
}

// Step 2: Wait for primary election (max 60 seconds)
print("=== Waiting for primary election ===");
let attempts = 0;
const maxAttempts = 60;
while (attempts < maxAttempts) {
  let ready = false;
  try {
    const hello = db.hello();
    if (hello.isWritablePrimary) {
      ready = true;
    }
  } catch (e) {
    // Ignore and retry
  }

  if (ready) {
    print("Primary elected and ready for writes!");
    break;
  }
  sleep(1000);
  attempts++;
}

if (attempts >= maxAttempts) {
  print("ERROR: Primary election timed out after " + maxAttempts + " seconds");
  quit(1);
}

// Step 3: Switch to appdb
print("=== Creating database: appdb ===");
const appdb = db.getSiblingDB("appdb");

// Step 4: Create collections
print("=== Creating collections: users, products, orders ===");
appdb.createCollection("users");
appdb.createCollection("products");
appdb.createCollection("orders");

// Step 5: Insert sample documents — users
print("=== Inserting sample users ===");
appdb.users.insertMany([
  {
    name: "Alice Johnson",
    email: "alice@example.com",
    age: 29,
    role: "engineer",
    createdAt: new Date()
  },
  {
    name: "Bob Smith",
    email: "bob@example.com",
    age: 34,
    role: "designer",
    createdAt: new Date()
  },
  {
    name: "Charlie Brown",
    email: "charlie@example.com",
    age: 42,
    role: "manager",
    createdAt: new Date()
  },
  {
    name: "Diana Ross",
    email: "diana@example.com",
    age: 27,
    role: "analyst",
    createdAt: new Date()
  },
  {
    name: "Eve Martinez",
    email: "eve@example.com",
    age: 31,
    role: "devops",
    createdAt: new Date()
  }
]);

// Step 6: Insert sample documents — products
print("=== Inserting sample products ===");
appdb.products.insertMany([
  {
    name: "Wireless Mouse",
    sku: "WM-001",
    price: 29.99,
    category: "electronics",
    stock: 150,
    createdAt: new Date()
  },
  {
    name: "Mechanical Keyboard",
    sku: "MK-002",
    price: 89.99,
    category: "electronics",
    stock: 75,
    createdAt: new Date()
  },
  {
    name: "USB-C Hub",
    sku: "UH-003",
    price: 45.00,
    category: "accessories",
    stock: 200,
    createdAt: new Date()
  },
  {
    name: "Monitor Stand",
    sku: "MS-004",
    price: 55.50,
    category: "furniture",
    stock: 60,
    createdAt: new Date()
  },
  {
    name: "Webcam HD",
    sku: "WC-005",
    price: 69.99,
    category: "electronics",
    stock: 120,
    createdAt: new Date()
  }
]);

// Step 7: Insert sample documents — orders
print("=== Inserting sample orders ===");
appdb.orders.insertMany([
  {
    orderId: "ORD-1001",
    userId: "alice@example.com",
    items: [{ sku: "WM-001", qty: 2 }],
    total: 59.98,
    status: "shipped",
    createdAt: new Date()
  },
  {
    orderId: "ORD-1002",
    userId: "bob@example.com",
    items: [{ sku: "MK-002", qty: 1 }],
    total: 89.99,
    status: "processing",
    createdAt: new Date()
  },
  {
    orderId: "ORD-1003",
    userId: "charlie@example.com",
    items: [{ sku: "UH-003", qty: 3 }, { sku: "MS-004", qty: 1 }],
    total: 190.50,
    status: "delivered",
    createdAt: new Date()
  },
  {
    orderId: "ORD-1004",
    userId: "diana@example.com",
    items: [{ sku: "WC-005", qty: 1 }],
    total: 69.99,
    status: "pending",
    createdAt: new Date()
  },
  {
    orderId: "ORD-1005",
    userId: "eve@example.com",
    items: [{ sku: "WM-001", qty: 1 }, { sku: "MK-002", qty: 1 }],
    total: 119.98,
    status: "shipped",
    createdAt: new Date()
  }
]);

print("=== MongoDB initialization complete ===");
print("  - Replica Set: rs0 (PRIMARY)");
print("  - Database:    appdb");
print("  - Collections: users (5), products (5), orders (5)");
