import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

export default withMermaid(defineConfig({
  title: "DataVo DBMS",
  description: "A Custom C# Database Management System",
  themeConfig: {
    nav: [
      { text: 'Home', link: '/' }
    ],
    sidebar: [
      {
        text: 'Architecture',
        items: [
          {
            text: 'DataVo.Core Modules',
            link: '/DataVo.Core/index',
            collapsed: false,
            items: [
              {
                text: 'BTree',
                collapsed: true,
                items: [
                  { text: 'Overview', link: '/DataVo.Core/BTree/index' },
                  { text: 'IndexManager', link: '/DataVo.Core/BTree/IndexManager' },
                  { text: 'BPlus', link: '/DataVo.Core/BTree/BPlus/index' },
                  { text: 'Binary', link: '/DataVo.Core/BTree/Binary/index' },
                  { text: 'Core', link: '/DataVo.Core/BTree/Core/index' }
                ]
              },
              { text: 'Cache', link: '/DataVo.Core/Cache/index' },
              { text: 'Constants', link: '/DataVo.Core/Constants/index' },
              {
                text: 'Contracts',
                collapsed: true,
                items: [
                  { text: 'Overview', link: '/DataVo.Core/Contracts/index' },
                  { text: 'Results', link: '/DataVo.Core/Contracts/Results/index' }
                ]
              },
              { text: 'Enums', link: '/DataVo.Core/Enums/index' },
              { text: 'Exceptions', link: '/DataVo.Core/Exceptions/index' },
              { text: 'Logger', link: '/DataVo.Core/Logger/index' },
              {
                text: 'Models',
                collapsed: true,
                items: [
                  { text: 'Overview', link: '/DataVo.Core/Models/index' },
                  { text: 'Catalog', link: '/DataVo.Core/Models/Catalog/index' },
                  { text: 'DDL', link: '/DataVo.Core/Models/DDL/index' },
                  { text: 'DML', link: '/DataVo.Core/Models/DML/index' },
                  { text: 'DQL', link: '/DataVo.Core/Models/DQL/index' },
                  { text: 'Statement', link: '/DataVo.Core/Models/Statement/index' },
                  { text: 'Statement Utils', link: '/DataVo.Core/Models/Statement/Utils/index' }
                ]
              },
              {
                text: 'Parser',
                collapsed: true,
                items: [
                  { text: 'Overview', link: '/DataVo.Core/Parser/index' },
                  { text: 'Parser Orchestrator', link: '/DataVo.Core/Parser/Parser' },
                  { text: 'AST', link: '/DataVo.Core/Parser/AST/index' },
                  { text: 'Actions', link: '/DataVo.Core/Parser/Actions/index' },
                  { text: 'Aggregations', link: '/DataVo.Core/Parser/Aggregations/index' },
                  { text: 'Binding', link: '/DataVo.Core/Parser/Binding/index' },
                  { text: 'Commands', link: '/DataVo.Core/Parser/Commands/index' },
                  {
                    text: 'DDL',
                    collapsed: true,
                    items: [
                      { text: 'Overview', link: '/DataVo.Core/Parser/DDL/index' },
                      { text: 'CreateTable', link: '/DataVo.Core/Parser/DDL/CreateTable' },
                      { text: 'DropTable', link: '/DataVo.Core/Parser/DDL/DropTable' },
                      { text: 'CreateIndex', link: '/DataVo.Core/Parser/DDL/CreateIndex' }
                    ]
                  },
                  {
                    text: 'DML',
                    collapsed: true,
                    items: [
                      { text: 'Overview', link: '/DataVo.Core/Parser/DML/index' },
                      { text: 'InsertInto', link: '/DataVo.Core/Parser/DML/InsertInto' },
                      { text: 'Update', link: '/DataVo.Core/Parser/DML/Update' },
                      { text: 'DeleteFrom', link: '/DataVo.Core/Parser/DML/DeleteFrom' },
                      { text: 'Vacuum', link: '/DataVo.Core/Parser/DML/Vacuum' }
                    ]
                  },
                  {
                    text: 'DQL',
                    collapsed: true,
                    items: [
                      { text: 'Overview', link: '/DataVo.Core/Parser/DQL/index' },
                      { text: 'Select', link: '/DataVo.Core/Parser/DQL/Select' }
                    ]
                  },
                  { text: 'Statements', link: '/DataVo.Core/Parser/Statements/index' },
                  { text: 'JoinStrategies', link: '/DataVo.Core/Parser/Statements/JoinStrategies/index' },
                  {
                    text: 'Mechanism',
                    collapsed: true,
                    items: [
                      { text: 'Overview', link: '/DataVo.Core/Parser/Statements/Mechanism/index' },
                      { text: 'StatementEvaluator', link: '/DataVo.Core/Parser/Statements/Mechanism/StatementEvaluator' }
                    ]
                  },
                  { text: 'Types', link: '/DataVo.Core/Parser/Types/index' },
                  { text: 'Utils', link: '/DataVo.Core/Parser/Utils/index' }
                ]
              },
              { text: 'Services', link: '/DataVo.Core/Services/index' },
              {
                text: 'StorageEngine',
                collapsed: true,
                items: [
                  { text: 'Overview', link: '/DataVo.Core/StorageEngine/index' },
                  { text: 'Config', link: '/DataVo.Core/StorageEngine/Config/index' },
                  {
                    text: 'Disk',
                    collapsed: true,
                    items: [
                      { text: 'Overview', link: '/DataVo.Core/StorageEngine/Disk/index' },
                      { text: 'DiskStorageEngine', link: '/DataVo.Core/StorageEngine/Disk/DiskStorageEngine' }
                    ]
                  },
                  {
                    text: 'Memory',
                    collapsed: true,
                    items: [
                      { text: 'Overview', link: '/DataVo.Core/StorageEngine/Memory/index' },
                      { text: 'InMemoryStorageEngine', link: '/DataVo.Core/StorageEngine/Memory/InMemoryStorageEngine' }
                    ]
                  },
                  { text: 'Serialization', link: '/DataVo.Core/StorageEngine/Serialization/index' }
                ]
              },
              { text: 'Utils', link: '/DataVo.Core/Utils/index' }
            ]
          }
        ]
      }
    ]
  }
})
)
