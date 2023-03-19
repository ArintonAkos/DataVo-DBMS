using Server.Enums;
using Server.Models.Catalog;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Net.NetworkInformation;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Server.Models.Factories
{
    internal class DataFactory
    {
        private Field Field { get; set; }
        private String ColumnName { get; set; }
        private String RawValue { get; set; }

        public static DataFactory Create()
        {
            return new DataFactory();
        }

        public DataFactory SetRawValue(String rawValue)
        {
            this.RawValue = rawValue;

            return this;
        }

        public DataFactory SetColumnName(String columnName)
        {
            ColumnName = columnName;

            return this;
        }

        public DataFactory LinkTable(Table table)
        {
            if (ColumnName == null)
            {
                throw new Exception("Column name must be defined before ");
            }

            Field = table.GetColumn(ColumnName);

            return this;
        }

        public Data Build()
        {
            if (Field == null)
            {
                throw new Exception("The table wasn't linked with this data!");
            }

            return new Data()
            {
                Type = Field.Type,
                Value = ParseRawValue()
            };
        }

        private dynamic ParseRawValue()
        {
            switch (Field.Type)
            {
                case DataTypes.Varchar:
                    // TO-DO: Do we have to store the splitted data if the string is longer than the varchar length or
                    // should we thow an exception?
                    return RawValue[..Field.Length];
                case DataTypes.Int:
                    try
                    {
                        return int.Parse(RawValue);
                    }
                    catch (Exception)
                    {
                        throw new Exception($"Trying to parse an integer value from {RawValue}");
                    }
                case DataTypes.Float:
                    try
                    {
                        return float.Parse(RawValue);
                    }
                    catch (Exception)
                    {
                        throw new Exception($"Trying to parse a float value from {RawValue}");
                    }
                default:
                    throw new Exception("Unsupported type!");
            }
        }
    }
}