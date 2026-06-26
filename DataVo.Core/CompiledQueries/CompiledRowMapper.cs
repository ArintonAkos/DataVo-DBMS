namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Projects one stored row into <typeparamref name="T"/> via a <see cref="CompiledRowReader"/>. A custom
/// delegate (rather than <c>Func&lt;,&gt;</c>) because the reader is a <c>ref struct</c>, which cannot be a
/// generic type argument. Source-generated instances are cached static fields — no per-row allocation.
/// </summary>
public delegate T CompiledRowMapper<T>(CompiledRowReader reader);
