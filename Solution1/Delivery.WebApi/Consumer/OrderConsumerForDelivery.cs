using Delivery.WebApi.IRepository;
using Delivery.WebApi.Models;
using MassTransit;
using Newtonsoft.Json;
using SharedService;

namespace Delivery.WebApi.Consumer
{
    public class OrderConsumerForDelivery : IConsumer<OrderPlaced>
    {
        private readonly IAssignDeliveryRepository _assignDeliveryRepository;


        public OrderConsumerForDelivery(IAssignDeliveryRepository assignDeliveryRepository)
        {
            _assignDeliveryRepository = assignDeliveryRepository;
        }
        public async Task Consume(ConsumeContext<OrderPlaced> context)
        {
            Console.WriteLine($"Order{JsonConvert.SerializeObject(context)}");
            //Console.WriteLine($"Order with Id {context.Message.OrderId} has been placed");
            //return Task.CompletedTask;

            DeliveryPartner deliveryPartner = await _assignDeliveryRepository.AvailableDeliveryPartner();
            if (deliveryPartner != null)
            {
                AssignedDelivery assignedDelivery = new AssignedDelivery(context.Message.OrderId, deliveryPartner.DeliveryPartnerName, deliveryPartner.PhoneNumber, deliveryPartner.Id);
                await _assignDeliveryRepository.MakeDeliveryPartnerFreeAndBusy(deliveryPartner.Id, false);
                await Task.Delay(TimeSpan.FromSeconds(25));
                await _assignDeliveryRepository.OrderDelivered(assignedDelivery);
                Console.WriteLine("OrderDelivered Successfully");
                await _assignDeliveryRepository.MakeDeliveryPartnerFreeAndBusy(deliveryPartner.Id,true);

            }
            else
            {
                Console.WriteLine($" No available delivery partners! Retrying order {context.Message.OrderId} in 10 seconds.");

                await context.Redeliver(TimeSpan.FromSeconds(10));
                //await context.Publish(context.Message);
            }
        }
    }
}
