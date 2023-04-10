# DataVo Database Server

This repository contains the server-side code for the DataVo project, a powerful and flexible database server built with C#. The DataVo server offers a robust and high-performance solution for managing and accessing your data with ease.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## Features

- High-performance, multi-threaded C# server.
- Customizable and extensible database schema.
- Support for various data types and relations.
- Powerful query language for data manipulation and retrieval.
- Built-in logging and exception handling.
- Modular architecture to facilitate future enhancements.

## Requirements

- .NET 6 or later
- MongoDB server

## Installation

1. Clone the repository:

```bash
git clone https://github.com/ArintonAkos/ABKR.git
```

2. Change to the project directory:

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

The server should now be running and listening for incoming connections.

## Usage

You can now interact with the DataVo server using the provided client libraries or by implementing your own client application. The server will respond to requests sent over the network using the DataVo query language.

## Project Structure

- `Enums/`: Contains various enumeration types used throughout the project.
- `Exceptions/`: Contains custom exception classes for error handling.
- `Logger/`: Provides a logging mechanism for debugging and monitoring server performance.
- `Models/`: Contains data model classes for the database schema.
- `Parser/`: Implements the parsing and execution of DataVo query language commands.
- `Properties/`: Contains project properties and settings.
- `Server/`: Houses the core server logic, including networking and request handling.
- `Utils/`: Contains utility classes and helper functions.
- `Program.cs`: The main entry point for the server application.
- `Server.csproj`: The project file for the DataVo server.

## Contributing

Contributions to the project are welcome. Please follow these steps to contribute:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Commit your changes to the new branch.
4. Create a pull request with a detailed description of your changes.

## License

This project is licensed under the [MIT License](LICENSE).