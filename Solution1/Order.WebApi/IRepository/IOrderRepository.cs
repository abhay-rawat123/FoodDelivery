using Order.WebApi.Models;
using SharedService;

namespace Order.WebApi.IRepository
{
    public interface IOrderRepository
    {
        Task AddOrderAsync(OrderRequest order);
        Task<List<OrderRequest>> GetOrdersAsync();
    }
}
