using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.PortableConsumer;

public static class PortableConsumer
{
    public static DataVoDate Exercise(DataVoDate value)
    {
        CellValue cell = CellValue.From(value);
        _ = cell.ToObject();

        using var storage = new StorageContext(new DataVoConfig
        {
            StorageMode = StorageMode.InMemory
        });

        return cell.AsDate();
    }
}
