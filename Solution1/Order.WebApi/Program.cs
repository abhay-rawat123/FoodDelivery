using Order.WebApi.IRepository;
using Order.WebApi.Repository;
using Order.WebApi.Models;
using SharedService;
using Microsoft.AspNetCore.Mvc;
using MassTransit;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Bson.Serialization;
using MongoDB.Bson;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using System.Text;

var builder = WebApplication.CreateBuilder(args);
var configuration = builder.Configuration;

// Register dependencies
BsonSerializer.RegisterSerializer(new GuidSerializer(GuidRepresentation.Standard));
// Configure MassTransit with RabbitMQ
builder.Services.AddMassTransit((x) =>
{

    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.Host("host.docker.internal", h =>
        {
            h.Username("guest");
            h.Password("guest");
        });

        cfg.Message<OrderPlaced>(e =>
        {
            
            e.SetEntityName("order-exchange");
        });
        cfg.Publish<OrderPlaced>(e =>
        {
            e.ExchangeType = "direct";
        });
    });
});
builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.RequireHttpsMetadata = false;
        options.SaveToken = true;
        options.TokenValidationParameters = new TokenValidationParameters
        {
            ValidateIssuer = true,
            ValidateAudience = true,
            ValidateLifetime = true,
            ValidateIssuerSigningKey = true,
            ValidIssuer = configuration["Jwt:Issuer"],
            ValidAudience = configuration["Jwt:Audience"],
            IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(configuration["Jwt:SecretKey"]))
        };
    });
// Add controllers and Swagger
builder.Services.AddScoped<IOrderRepository, OrderRepository>();
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();


var app = builder.Build();

// Configure middleware
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();
app.UseAuthentication();   
app.UseAuthorization();
app.MapControllers();

app.Run();
