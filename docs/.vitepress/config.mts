import { defineConfig } from "vitepress";
import { withMermaid } from "vitepress-plugin-mermaid";

export default withMermaid(
  defineConfig({
    title: "DataVo DBMS",
    description: "A Custom C# Database Management System",
    themeConfig: {
      nav: [{ text: "Home", link: "/" }],
      sidebar: [
        {
          text: "Architecture",
          items: [
            {
              text: "DataVo.Core Modules",
              link: "/DataVo.Core/",
              collapsed: false,
              items: [
                {
                  text: "BTree",
                  collapsed: true,
                  items: [
                    { text: "Overview", link: "/DataVo.Core/BTree/" },
                    {
                      text: "IndexManager",
                      link: "/DataVo.Core/BTree/IndexManager",
                    },
                    { text: "BPlus", link: "/DataVo.Core/BTree/BPlus/" },
                    { text: "Binary", link: "/DataVo.Core/BTree/Binary/" },
                    { text: "Core", link: "/DataVo.Core/BTree/Core/" },
                  ],
                },
                { text: "Cache", link: "/DataVo.Core/Cache/" },
                { text: "Constants", link: "/DataVo.Core/Constants/" },
                {
                  text: "Contracts",
                  collapsed: true,
                  items: [
                    { text: "Overview", link: "/DataVo.Core/Contracts/" },
                    {
                      text: "Results",
                      link: "/DataVo.Core/Contracts/Results/",
                    },
                  ],
                },
                { text: "Enums", link: "/DataVo.Core/Enums/" },
                { text: "Exceptions", link: "/DataVo.Core/Exceptions/" },
                { text: "Logger", link: "/DataVo.Core/Logger/" },
                {
                  text: "Models",
                  collapsed: true,
                  items: [
                    { text: "Overview", link: "/DataVo.Core/Models/" },
                    { text: "Catalog", link: "/DataVo.Core/Models/Catalog/" },
                    { text: "DDL", link: "/DataVo.Core/Models/DDL/" },
                    { text: "DML", link: "/DataVo.Core/Models/DML/" },
                    { text: "DQL", link: "/DataVo.Core/Models/DQL/" },
                    {
                      text: "Statement",
                      link: "/DataVo.Core/Models/Statement/",
                    },
                    {
                      text: "Statement Utils",
                      link: "/DataVo.Core/Models/Statement/Utils/",
                    },
                  ],
                },
                {
                  text: "Parser",
                  collapsed: true,
                  items: [
                    { text: "Overview", link: "/DataVo.Core/Parser/" },
                    {
                      text: "Parser Orchestrator",
                      link: "/DataVo.Core/Parser/Parser",
                    },
                    { text: "AST", link: "/DataVo.Core/Parser/AST/" },
                    { text: "Actions", link: "/DataVo.Core/Parser/Actions/" },
                    {
                      text: "Aggregations",
                      link: "/DataVo.Core/Parser/Aggregations/",
                    },
                    { text: "Binding", link: "/DataVo.Core/Parser/Binding/" },
                    { text: "Commands", link: "/DataVo.Core/Parser/Commands/" },
                    {
                      text: "DDL",
                      collapsed: true,
                      items: [
                        { text: "Overview", link: "/DataVo.Core/Parser/DDL/" },
                        {
                          text: "CreateTable",
                          link: "/DataVo.Core/Parser/DDL/CreateTable",
                        },
                        {
                          text: "DropTable",
                          link: "/DataVo.Core/Parser/DDL/DropTable",
                        },
                        {
                          text: "CreateIndex",
                          link: "/DataVo.Core/Parser/DDL/CreateIndex",
                        },
                      ],
                    },
                    {
                      text: "DML",
                      collapsed: true,
                      items: [
                        { text: "Overview", link: "/DataVo.Core/Parser/DML/" },
                        {
                          text: "InsertInto",
                          link: "/DataVo.Core/Parser/DML/InsertInto",
                        },
                        {
                          text: "Update",
                          link: "/DataVo.Core/Parser/DML/Update",
                        },
                        {
                          text: "DeleteFrom",
                          link: "/DataVo.Core/Parser/DML/DeleteFrom",
                        },
                        {
                          text: "Vacuum",
                          link: "/DataVo.Core/Parser/DML/Vacuum",
                        },
                      ],
                    },
                    {
                      text: "DQL",
                      collapsed: true,
                      items: [
                        { text: "Overview", link: "/DataVo.Core/Parser/DQL/" },
                        {
                          text: "Select",
                          link: "/DataVo.Core/Parser/DQL/Select",
                        },
                      ],
                    },
                    {
                      text: "Transactions",
                      link: "/DataVo.Core/Parser/Transactions/",
                    },
                    {
                      text: "Statements",
                      link: "/DataVo.Core/Parser/Statements/",
                    },
                    {
                      text: "JoinStrategies",
                      link: "/DataVo.Core/Parser/Statements/JoinStrategies/",
                    },
                    {
                      text: "Mechanism",
                      collapsed: true,
                      items: [
                        {
                          text: "Overview",
                          link: "/DataVo.Core/Parser/Statements/Mechanism/",
                        },
                        {
                          text: "StatementEvaluator",
                          link: "/DataVo.Core/Parser/Statements/Mechanism/StatementEvaluator",
                        },
                      ],
                    },
                    { text: "Types", link: "/DataVo.Core/Parser/Types/" },
                    { text: "Utils", link: "/DataVo.Core/Parser/Utils/" },
                  ],
                },
                { text: "Transactions", link: "/DataVo.Core/Transactions/" },
                { text: "Services", link: "/DataVo.Core/Services/" },
                {
                  text: "StorageEngine",
                  collapsed: true,
                  items: [
                    { text: "Overview", link: "/DataVo.Core/StorageEngine/" },
                    {
                      text: "Config",
                      link: "/DataVo.Core/StorageEngine/Config/",
                    },
                    {
                      text: "Disk",
                      collapsed: true,
                      items: [
                        {
                          text: "Overview",
                          link: "/DataVo.Core/StorageEngine/Disk/",
                        },
                        {
                          text: "DiskStorageEngine",
                          link: "/DataVo.Core/StorageEngine/Disk/DiskStorageEngine",
                        },
                      ],
                    },
                    {
                      text: "Memory",
                      collapsed: true,
                      items: [
                        {
                          text: "Overview",
                          link: "/DataVo.Core/StorageEngine/Memory/",
                        },
                        {
                          text: "InMemoryStorageEngine",
                          link: "/DataVo.Core/StorageEngine/Memory/InMemoryStorageEngine",
                        },
                      ],
                    },
                    {
                      text: "Serialization",
                      link: "/DataVo.Core/StorageEngine/Serialization/",
                    },
                  ],
                },
                { text: "Utils", link: "/DataVo.Core/Utils/" },
              ],
            },
          ],
        },
      ],
    },
  }),
);
