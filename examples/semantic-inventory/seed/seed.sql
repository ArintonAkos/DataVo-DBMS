CREATE DATABASE SemanticInventory;
USE SemanticInventory;

CREATE TABLE Items (
  Id INT PRIMARY KEY,
  Name VARCHAR,
  Category VARCHAR,
  Description VARCHAR
);

CREATE TABLE Inventory (
  ItemId INT PRIMARY KEY,
  Quantity INT,
  Location VARCHAR
);

CREATE TABLE Sales (
  Id INT PRIMARY KEY,
  ItemId INT,
  Qty INT,
  Price FLOAT
);

CREATE TABLE ItemEmbeddings (
  ItemId INT PRIMARY KEY,
  Emb VECTOR(3)
);

INSERT INTO Items (Id, Name, Category, Description) VALUES (1, 'Chair', 'Furniture', 'Ergonomic office chair');
INSERT INTO Items (Id, Name, Category, Description) VALUES (2, 'Table', 'Furniture', 'Wooden desk table');
INSERT INTO Items (Id, Name, Category, Description) VALUES (3, 'Lamp', 'Lighting', 'Warm desk lamp');

INSERT INTO Inventory (ItemId, Quantity, Location) VALUES (1, 42, 'A1');
INSERT INTO Inventory (ItemId, Quantity, Location) VALUES (2, 25, 'A2');
INSERT INTO Inventory (ItemId, Quantity, Location) VALUES (3, 68, 'B1');

INSERT INTO ItemEmbeddings (ItemId, Emb) VALUES (1, '[1,0,0]');
INSERT INTO ItemEmbeddings (ItemId, Emb) VALUES (2, '[0,1,0]');
INSERT INTO ItemEmbeddings (ItemId, Emb) VALUES (3, '[0.9,0.1,0]');

CREATE INDEX idx_item_embeddings ON ItemEmbeddings (Emb) USING HNSW;
