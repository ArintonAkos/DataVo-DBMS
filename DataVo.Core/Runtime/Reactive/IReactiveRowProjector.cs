using System.ComponentModel;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Projects a borrowed reactive <see cref="RowRef"/> into a strongly-typed <typeparamref name="TProjection"/>
/// without boxing or heap allocation. Implementations are value-type structs emitted by <c>DataVo.Generators</c>.
/// </summary>
/// <typeparam name="TProjection">The projected value type or model.</typeparam>
/// <example>
/// <code>
/// readonly struct HudSlotProjector : IReactiveRowProjector&lt;HudSlot&gt;
/// {
///     public HudSlot Project(in RowRef row) => new(row.ReadInt32(0, "Id"), row.ReadInt32(1, "Score"));
/// }
/// </code>
/// </example>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface IReactiveRowProjector<out TProjection>
{
    /// <summary>
    /// Projects <paramref name="row"/> into a <typeparamref name="TProjection"/> using pre-resolved column ordinals.
    /// </summary>
    /// <param name="row">The borrowed reactive row whose cells are ordered by the projection's column list.</param>
    /// <returns>The projected value.</returns>
    TProjection Project(in RowRef row);
}

/// <summary>
/// Builds a <c>[DataVoTable]</c> model directly from a borrowed reactive <see cref="RowRef"/> without
/// dictionary materialization or boxing. Generated code emits one per model.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
/// <param name="row">The borrowed reactive row whose cells follow the model's column order.</param>
/// <returns>The materialized model instance.</returns>
[EditorBrowsable(EditorBrowsableState.Never)]
public delegate T DataVoReactiveRowReader<out T>(in RowRef row);
