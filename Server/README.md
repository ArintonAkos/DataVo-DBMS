# DataVo Server

> Support scope: this server is a local development/demo host for DataVo APIs. It is not a hardened multi-tenant production API. Keep it bound to localhost unless you add authentication, deployment-grade CORS policy, deployment-grade request limits, logging, and operational controls.

DataVo Server exposes a small HTTP interface over the DataVo query engine. It is useful for local demos, browser experiments, and API prototyping.

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

- C# and .NET 10
- `HttpListener`
- DataVo.Core query execution
- Newtonsoft.Json for request and response JSON

## System Requirements

To run this project, you need the .NET 10 SDK.

## Installation and Setup

You can get the project up and running in a few steps:

1. Clone the repository:

```bash
git clone https://github.com/ArintonAkos/DataVo-DBMS.git
```

2. Navigate to the project directory:

```bash
cd DataVo-DBMS
```

3. Build the project:

```bash
dotnet build Server/Server.csproj
```

4. Run the project:

```bash
dotnet run --project Server/Server.csproj
```

The server listens on `http://localhost:8001/`.

## Usage

Once the server is running, local clients can send HTTP requests to the DataVo endpoints. The parser endpoint accepts SQL text and a session identifier in JSON.

Runtime settings:

- `DATAVO_SERVER_CORS_ORIGIN`: allowed browser origin. Defaults to `http://localhost:5173`.
- `DATAVO_SERVER_MAX_BODY_BYTES`: maximum POST body size in bytes. Defaults to `1048576`.


## Detailed Usage Guide

This section describes the typical workflow of the DataVo Database Server, alongside examples of how to interact with it.

### Starting the Server

After installing and setting up the project following the instructions above, you can start the server by running the following command in the project directory:

```bash
dotnet run --project Server/Server.csproj
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

- If the dotnet run command fails with an SDK error, make sure that the .NET 10 SDK is installed.
  You can check your .NET version by running dotnet --version.
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

The demo server keeps one background task per accepted request and bounds POST body reads by default. CORS defaults to a local frontend origin instead of `*`.

This is not a production hardening boundary. Before exposing it beyond localhost, add authentication, authorization, structured logging, TLS termination, rate limiting, deployment-grade CORS policy, and operational monitoring.

## Future Work

We're constantly working to improve the DataVo server and add new features. Some of the things we're planning for future releases include:

- More advanced query capabilities, such as subqueries and complex join operations.
- A graphical interface for managing the server queries and viewing database schema and design.
- More comprehensive performance metrics and tuning options.
- `INSERT INTO table VALUES (...)` support that reliably maps to catalog column order when no column list is provided.
- Partial-column INSERT support with catalog-backed defaults for omitted columns.
- Auto-increment/identity column support so generated keys do not need to be explicitly provided on INSERT.

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

The use of C# and .NET 10 keeps the server aligned with the rest of the DataVo engine and test matrix.

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
