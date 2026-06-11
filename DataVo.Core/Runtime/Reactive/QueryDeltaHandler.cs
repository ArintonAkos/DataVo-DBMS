namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Receives a borrowed reactive delta on the zero-allocation fast lane. The <see cref="QueryChangeRef"/>
/// is valid only for the duration of the call; retain data via <see cref="QueryChangeRef.Materialize"/>
/// or by copying out the cells you need.
/// </summary>
public delegate void QueryDeltaHandler(in QueryChangeRef change);
