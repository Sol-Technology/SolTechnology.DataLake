using SolTechnology.DataLake;

// The namespace of IServiceCollection, so AddDataLake shows up next to AddDbContext and friends.
namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers <see cref="LakeContext"/>s, one registration per context class, like <c>AddDbContext</c>.</summary>
public static class DataLakeServiceCollectionExtensions
{
    /// <summary>
    /// Registers <typeparamref name="TContext"/> as a singleton (a context holds no per-request state). Two technologies
    /// side by side are two context classes, each registered on its own.
    /// </summary>
    public static IServiceCollection AddDataLake<TContext>(this IServiceCollection services, Action<LakeContextOptionsBuilder> configure)
        where TContext : LakeContext
    {
        ArgumentNullException.ThrowIfNull(configure);
        return services.AddDataLake<TContext>((_, options) => configure(options));
    }

    /// <inheritdoc cref="AddDataLake{TContext}(IServiceCollection, Action{LakeContextOptionsBuilder})"/>
    /// <param name="services">The services.</param>
    /// <param name="configure">Gets the service provider, for clients registered elsewhere (<c>IAmazonS3</c>, <c>BlobServiceClient</c>).</param>
    public static IServiceCollection AddDataLake<TContext>(this IServiceCollection services, Action<IServiceProvider, LakeContextOptionsBuilder> configure)
        where TContext : LakeContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        services.AddSingleton(sp => LakeContextOptions.For<TContext>(options => configure(sp, options)));
        services.AddSingleton<TContext>();
        return services;
    }

    /// <summary>Registers <typeparamref name="TContext"/> and resolves <typeparamref name="TService"/> (your interface) to the same instance.</summary>
    public static IServiceCollection AddDataLake<TService, TContext>(this IServiceCollection services, Action<LakeContextOptionsBuilder> configure)
        where TService : class
        where TContext : LakeContext, TService
    {
        ArgumentNullException.ThrowIfNull(configure);
        return services.AddDataLake<TService, TContext>((_, options) => configure(options));
    }

    /// <inheritdoc cref="AddDataLake{TService, TContext}(IServiceCollection, Action{LakeContextOptionsBuilder})"/>
    /// <param name="services">The services.</param>
    /// <param name="configure">Gets the service provider, for clients registered elsewhere (<c>IAmazonS3</c>, <c>BlobServiceClient</c>).</param>
    public static IServiceCollection AddDataLake<TService, TContext>(this IServiceCollection services, Action<IServiceProvider, LakeContextOptionsBuilder> configure)
        where TService : class
        where TContext : LakeContext, TService
    {
        services.AddDataLake<TContext>(configure);
        services.AddSingleton<TService>(sp => sp.GetRequiredService<TContext>());
        return services;
    }
}
