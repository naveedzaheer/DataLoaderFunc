using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

[assembly: FunctionsStartup(typeof(DataLoaderFunc.Startup))]

namespace DataLoaderFunc
{
    /// <summary>
    /// IoC Startup Class (Required)
    /// </summary>
    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddSingleton<IConfigCacheClient>((s) => {
                return new ConfigCacheClient();
            });
        }
    }
}
