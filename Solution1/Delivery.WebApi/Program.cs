using Delivery.WebApi.Consumer;
using Delivery.WebApi.IRepository;
using Delivery.WebApi.Repository;
using MassTransit;

var builder = WebApplication.CreateBuilder(args);
// Add services to the container.
builder.Services.AddMassTransit((x) =>
{
    x.AddConsumer<OrderConsumerForDelivery>();

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.Host("host.docker.internal", h =>
        {
            h.Username("guest");
            h.Password("guest");
        });

        cfg.ReceiveEndpoint("order-queue", e =>
        {
            e.UseScheduledRedelivery(r => r.Intervals(TimeSpan.FromSeconds(20))); // Delay failed messages
            e.UseMessageRetry(r => r.Intervals(10000, 20000, 30000));
            e.UseMessageRetry(r => r.Intervals(20000)); // Retry after 10 seconds
            e.Consumer<OrderConsumerForDelivery>(context);
            e.Bind("order-exchange", x =>
            {
                x.RoutingKey = "order.placed";
                x.ExchangeType = "direct";
            });
        });
    });
}).AddLogging();
builder.Services.AddScoped<IAssignDeliveryRepository, AssignDeliveryRepository>();
builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
