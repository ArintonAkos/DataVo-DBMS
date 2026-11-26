using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using UnityEngine;

namespace DataVo.Samples
{
    /// <summary>Attach to any GameObject to create a table and read a row back when the scene starts.</summary>
    public sealed class DataVoQuickStart : MonoBehaviour
    {
        private void Start()
        {
            using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
            db.Execute("CREATE DATABASE Game");
            db.Execute("USE Game");
            db.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(40))");
            db.Execute("INSERT INTO Items (Id, Name) VALUES (1, 'Sword')");

            DataVoPreparedStatement lookup = db.Prepare("SELECT Name FROM Items WHERE Id = @id");
            QueryResult result = lookup.Execute(("id", 1));
            Debug.Log($"Item 1 is {result.Data[0]["Name"]}");
        }
    }
}
