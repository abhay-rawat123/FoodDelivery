namespace Delivery.WebApi.Models
{
    public class DeliveryPartner
    {
        public int Id { get; set; }
        public string DeliveryPartnerName { get; set; }
        public string  PhoneNumber { get; set; }
        public bool IsAvailable { get; set; }
    }
}
