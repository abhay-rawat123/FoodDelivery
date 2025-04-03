using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Order.WebApi.IRepository;
using Order.WebApi.Models;
using SharedService;

namespace Order.WebApi.Repository
{
    public class OrderRepository:IOrderRepository
    {
        private readonly IMongoCollection<OrderRequest> _ordersCollection;
      
        public OrderRepository(IConfiguration configuration)
        {
            var client = new MongoClient(configuration["MongoDB:ConnectionString"]);
            var database = client.GetDatabase(configuration["MongoDB:DatabaseName"]);
            _ordersCollection = database.GetCollection<OrderRequest>(configuration["MongoDB:OrdersCollection"]);
        }

        public async Task AddOrderAsync(OrderRequest order)
        {
            await _ordersCollection.InsertOneAsync(order);
        }

        public async Task<List<OrderRequest>> GetOrdersAsync()
        {
            return await _ordersCollection.Find(_ => true).ToListAsync();
        }
    }
}
