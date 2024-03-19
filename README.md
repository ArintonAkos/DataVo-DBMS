# DataVo Database Server

![img](https://github.com/ArintonAkos/ABKR/assets/43067524/5f90c2f1-71a9-42a5-9bac-5128750fc089)

DataVo is a high-performance, multi-threaded C# server providing robust functionalities for managing and manipulating data with a user-friendly interface. This document offers an in-depth look at the server's architecture, functionality, and usage.

**Image was generated using Bing Image Generator**

## Table of Contents

1. [Technologies Used](#technologies-used)
2. [System Requirements](#system-requirements)
3. [Installation and Setup](#installation-and-setup)
4. [Usage](#usage)
5. [Detailed Usage Guide](#detailed-usage-guide)
    - [Starting the Server](#starting-the-server)
    - [Interacting with the Server](#interacting-with-the-server)
    - [Stopping the Server](#stopping-the-server)
    - [Troubleshooting](#troubleshooting)
6. [Advanced Usage](#advanced-usage)
    - [Aggregation](#aggregation)
    - [Joins](#joins)
    - [Error Handling and Debugging](#error-handling-and-debugging)
7. [Performance and Security](#performance-and-security)
8. [Future Work](#future-work)
9. [In-Depth Project Structure](#in-depth-project-structure)
10. [Application Design and Development](#application-design-and-development)
11. [Contributing](#contributing)
12. [License](#license)
13. [FAQs](#faqs)

## Technologies Used

- C#: C# is a modern, object-oriented programming language that offers a robust and versatile platform for building software applications. It was used in this project because it allows for fast development while maintaining type-safety and performance.

- .NET 6: .NET 6 is a cross-platform framework that allows for the development of modern applications. Its compatibility with C#, scalability, and high performance make it an ideal choice for this project.

- MongoDB: MongoDB is a source-available, NoSQL database that offers high performance, high availability, and easy scalability. It works on the concept of collections and documents.

## System Requirements

To run this project, you need:

- .NET 6 or later
- MongoDB server

## Installation and Setup

You can get the project up and running in a few steps:

1. Clone the repository:

```bash
git clone https://github.com/ArintonAkos/ABKR.git
```

2. Navigate to the project directory:

```bash
cd ABKR
```

3. Build the project:

```bash
dotnet build
```

4. Run the project:

```bash
dotnet run
```

The server is now running and ready to handle incoming connections.

## Usage

Once the server is up and running, it listens for incoming connections. You can interact with the DataVo server using provided client libraries or by implementing your own client application. The server responds to requests sent over the network using the DataVo query language.


## Detailed Usage Guide

This section describes the typical workflow of the DataVo Database Server, alongside examples of how to interact with it.

### Starting the Server

After installing and setting up the project following the instructions above, you can start the server by running the following command in the project directory:

```bash
dotnet run
```

### Interacting with the Server

The DataVo server communicates with client applications (DataVo DBMS) over the network using the HTTP protocol. 
This DataVo Query Language provides commands for creating, reading, updating, and deleting data in the database.
The server accepts various HTTP requests and responds with the appropriate data.

For example, to create a new database record, you might send a command like this:

```bash
CREATE DATABASE TesztAdatbazis;
```

In this command, CREATE is the action to be performed, TesztAdatbazis is the database on which the action is performed.

The server processes this command, performs the requested action in the database, 
and sends back a response indicating the result of the operation.

### Stopping the Server

To stop the server, simply press CTRL+C in the terminal where the server is running.

### Troubleshooting

If you encounter problems while setting up or running the server, here are some possible solutions:

- If the dotnet run command fails with an error saying that .NET 6 is not installed, make sure that you have .NET 6 or later installed on your machine. 
  You can check your .NET version by running dotnet --version.
- If the server fails to start and an error message indicates that it can't connect to the MongoDB server, make sure that your MongoDB server is running 
  and that the connection parameters are correctly configured in your server settings.
- If the server starts but you can't connect to it from your client application, make sure that your client is trying to connect to the correct IP address 
  and port. Also, ensure that your network allows connections on the server's port.

## Advanced Usage

The DataVo server not only provides basic CRUD operations, but also supports complex operations such as aggregation, joins, and group by clauses. 
In this section, we will describe how to utilize these features to perform advanced queries on your data.

### Aggregation

DataVo supports various aggregation functions, such as `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`. 
These can be used in combination with the `GROUP BY` clause to aggregate data over a specific column. Here is an example:

```sql
SELECT AVG(score), subject FROM Exams GROUP BY subject
```

This command would return the average score for each subject.

### Joins

DataVo supports the JOIN operation, which allows you to combine rows from two or more tables based on a related column. 
For example, you might want to join a table of employees with a table of departments, like this:

```sql
SELECT Employees.name, Departments.departmentName FROM Employees JOIN Departments ON Employees.departmentId = Departments.departmentId
```

### Error Handling and Debugging

DataVo server has a built-in logging mechanism that records all the actions performed on the server.
These logs are invaluable when troubleshooting errors or bugs.

In case of an error, the server responds with a descriptive error message. 
For example, if you attempt to create a record without providing necessary data, 
the server might respond with a message like "Missing data for required field 'name'."

## Performance and Security

The DataVo server is designed with performance and security in mind. It uses multi-threading to handle multiple connections simultaneously,
providing high throughput and low latency.

## Future Work

We're constantly working to improve the DataVo server and add new features. Some of the things we're planning for future releases include:

- More advanced query capabilities, such as subqueries and complex join operations.
- A graphical interface for managing the server queries and viewing database schema and design.
- More comprehensive performance metrics and tuning options.

## In-Depth Project Structure

The project is divided into several main components:

- **`Enums/`**: Contains various enumeration types used throughout the project. Enumerations offer a way of defining type-safe constants, enhancing code readability and correctness.
- **`Exceptions/`**: Houses custom exception classes that enable accurate error handling and debugging. Each exception class corresponds to a specific type of error that can occur during the execution of the server.
- **`Logger/`**: Provides functionalities for logging and monitoring server performance. It plays a critical role in maintenance and debugging by recording the operations and exceptions that occur during the server's runtime.
- **`Models/`**: Houses the classes that define the structure of the objects in the database. Each class corresponds to a particular entity in the database schema, defining its properties and constraints.
- **`Parser/`**: Implements the parsing and execution of the DataVo query language commands. It breaks down user inputs into recognizable commands and actions for the server to perform.
- **`Properties/`**: Contains project properties and settings, including global constants and default settings.
- **`Server/`**: The core server logic resides here, including networking and request handling. It is responsible for accepting and managing client connections, processing client requests, and sending back responses.
- **`Utils/`**: Contains utility classes and helper functions that assist in performing common tasks throughout the project.
- **`Program.cs`**: The main entry point for the server application. It initializes the server and triggers its execution.
- **`Server.csproj`**: The project file for the DataVo server. It lists project dependencies, versioning, and other configurations.

## Application Design and Development

This project utilizes a modular design pattern that makes the codebase scalable and maintainable. Each component of the project corresponds to a specific part of the server's functionality, making it easier to update and debug individual features without affecting the rest of the project.

The use of C# along with .NET 6 enhances the application's performance and efficiency, while MongoDB provides a robust and scalable database solution.

## Contributing

Contributions to the project are welcome. Please follow these steps to contribute:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Commit your changes to the new branch.
4. Create a pull request with a detailed description of your changes.

## License

This project is licensed under the MIT License. It permits use, duplication, modification, distribution, and private use of the software, subject to the conditions outlined in the license.

## FAQs

**Question**: How can I report a bug or suggest a feature?

**Answer**: To report a bug or suggest a feature, create an issue on the project's GitHub repository.

**Question**: Where can I find more information about the DataVo query language?

**Answer**: The DataVo query language documentation can be found in the project's GitHub repository.

**Question**: How do I contribute to the project?

**Answer**: Contributions are made via pull requests. After forking the repository and making your changes, create a new pull request and describe your changes.

If you have any suggestions for new features or improvements, feel free to open an issue on our GitHub repository. We'd love to hear from you!
