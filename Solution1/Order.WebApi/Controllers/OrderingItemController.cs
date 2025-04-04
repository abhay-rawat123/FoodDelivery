using MassTransit;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Order.WebApi.IRepository;
using Order.WebApi.Models;
using Order.WebApi.Repository;
using SharedService;

namespace Order.WebApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Authorize]
    public class OrderingItemController : ControllerBase
    {
        private readonly IBus _bus;
        private readonly IConfiguration _configuration;
        private readonly IOrderRepository _orderRepository;

        public OrderingItemController(IBus bus,IConfiguration configuration,IOrderRepository orderRepository)
        {
            _bus = bus;
            _configuration = configuration;
            _orderRepository = orderRepository;
        }
        [HttpPost]
        [Route("[action]")]
        public async Task<IActionResult> OrderItem([FromBody] OrderRequest order)
        {
            var orderPlacedMessage = new OrderPlaced(order.OrderId,order.RestaurantId, order.RestaurantName, order.ItemId,order.ItemName,order.ItemDescription);
            //await _bus.Publish(orderPlacedMessage);
            //direct
            await _orderRepository.AddOrderAsync(order);
            await _bus.Publish(orderPlacedMessage, context =>
            {
                context.SetRoutingKey("order.placed");
            }); 
            return Ok(new { Message = "Order Placed Successfully" });
        }
    }
}
