using Microsoft.EntityFrameworkCore;
using SemanticInventory.Backend.Components;
using SemanticInventory.Backend.Data;
using SemanticInventory.Backend.Services;

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseStaticWebAssets();

builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

string dbPath = Path.Combine(builder.Environment.ContentRootPath, "semantic-inventory.db");
builder.Services.AddDbContextFactory<SemanticInventoryContext>(options =>
    options.UseSqlite($"Data Source={dbPath}"));

builder.Services.Configure<EmbeddingOptions>(builder.Configuration.GetSection("Embeddings"));
builder.Services.AddHttpClient("ollama-embeddings");
builder.Services.AddCors(options =>
{
    options.AddPolicy("frontend", policy =>
    {
        policy.WithOrigins("http://localhost:5173", "http://127.0.0.1:5173")
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});

builder.Services.AddScoped<InventoryService>();
builder.Services.AddScoped<StressService>();
builder.Services.AddSingleton<ITextEmbeddingService, OllamaOrDeterministicEmbeddingService>();
builder.Services.AddScoped<EmbeddingPipelineService>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var contextFactory = scope.ServiceProvider.GetRequiredService<IDbContextFactory<SemanticInventoryContext>>();
    var embeddingPipeline = scope.ServiceProvider.GetRequiredService<EmbeddingPipelineService>();
    await using var context = await contextFactory.CreateDbContextAsync();
    await context.Database.MigrateAsync();
    int seedItemCount = builder.Configuration.GetValue<int?>("Seed:ItemCount") ?? 2500;
    await AppSeeder.SeedAsync(context, seedItemCount);
    await embeddingPipeline.EmbedAllItemsAsync();
}

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
}

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseAntiforgery();
app.UseCors("frontend");

app.MapGet("/api/catalog", async (InventoryService inventoryService, string? category, CancellationToken cancellationToken) =>
{
    var rows = await inventoryService.GetCatalogAsync(category, cancellationToken);
    return Results.Ok(rows);
});

app.MapGet("/api/top-sellers", async (InventoryService inventoryService, int? take, CancellationToken cancellationToken) =>
{
    int top = Math.Clamp(take ?? 10, 1, 100);
    var rows = await inventoryService.GetTopSellersAsync(top, cancellationToken);
    return Results.Ok(rows);
});

app.MapPost("/api/search", async (InventoryService inventoryService, SearchRequest request, CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(request.Query))
    {
        return Results.BadRequest(new { error = "Query is required." });
    }

    int topK = Math.Clamp(request.TopK, 1, 100);
    var rows = await inventoryService.SearchByTextAsync(request.Query, topK, cancellationToken);
    return Results.Ok(rows);
});

app.MapPost("/api/order", async (InventoryService inventoryService, OrderRequest request, CancellationToken cancellationToken) =>
{
    var result = await inventoryService.PlaceOrderAsync(request.ItemId, request.Qty, cancellationToken);
    return result.Success ? Results.Ok(result) : Results.BadRequest(result);
});

app.MapGet("/api/health", () => Results.Ok(new { ok = true }));

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

app.MapStaticAssets();

app.Run();

public sealed record SearchRequest(string Query, int TopK = 10);
public sealed record OrderRequest(int ItemId, int Qty);
