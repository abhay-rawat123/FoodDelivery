namespace Delivery.WebApi.Models
{
    public class AssignedDelivery
    {
       
            public Guid OrderId { get; set; }
            public int DeliveryPartnerId { get; set; }
            public string DeliveryPartner { get; set; }
            public string PhoneNumber { get; set; }

            public AssignedDelivery(Guid orderId, string deliveryPartner, string phoneNumber,int deliveryPartnerId)
            {
                OrderId = orderId;
                DeliveryPartner = deliveryPartner;
                PhoneNumber = phoneNumber;
            DeliveryPartnerId = deliveryPartnerId;
            }
        

    }
}
