using Delivery.WebApi.Models;
using SharedService;

namespace Delivery.WebApi.IRepository
{
    public interface IAssignDeliveryRepository
    {
        public Task<DeliveryPartner> AvailableDeliveryPartner();
        public Task OrderDelivered(AssignedDelivery delivery);
        Task MakeDeliveryPartnerFreeAndBusy(int deliveryPartnerId,bool IsAvailable);
    }
}
