
namespace DataVo.Core.Contracts
{
    /// <summary>
    /// Represents a column-like value that can expose its storage type in raw catalog form.
    /// </summary>
    public interface IColumn
    {
        /// <summary>
        /// Gets the original type name used by the storage and parsing layers.
        /// </summary>
        /// <returns>The raw type string, such as <c>INT</c> or <c>VARCHAR</c>.</returns>
        public abstract string RawType();
    }
}
