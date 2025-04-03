using Dapper;
using Delivery.WebApi.IRepository;
using Delivery.WebApi.Models;
using System.Data;

namespace Delivery.WebApi.Repository
{
    public class AssignDeliveryRepository(IConfiguration configuration):BaseRepository(configuration), IAssignDeliveryRepository
    {
        private readonly new IConfiguration _configuration=configuration;
        public async Task<DeliveryPartner> AvailableDeliveryPartner()
        {
            // Get User Details
            try
            {
                var parameters = new DynamicParameters();
                return await GetAsync<DeliveryPartner>("getAvailableDeliveryPartner", parameters, CommandType.StoredProcedure);
            }
            catch (Exception ex)
            {
                throw new Exception("Internal Server Error", ex);
            }
        }

        public async Task OrderDelivered(AssignedDelivery delivery)
        {
            try
            {
                var parameters = new DynamicParameters();
                parameters.Add("@OrderId", delivery.OrderId, DbType.Guid, ParameterDirection.Input);
                parameters.Add("@DeliveryPartnerId", delivery.DeliveryPartnerId, DbType.Int64, ParameterDirection.Input);

                await GetAsync<DeliveryPartner>("OrderDeliveredSuccessfully", parameters, CommandType.StoredProcedure);
            }
            catch (Exception ex) 
            {
                throw new Exception("Internal Server Error", ex);

            }

        }


        public async Task MakeDeliveryPartnerFreeAndBusy(int deliveryPartnerId,bool isAvailable)
        {
            try
            {
                var parameters = new DynamicParameters();
                parameters.Add("@DeliveryPartnerId", deliveryPartnerId, DbType.Int32, ParameterDirection.Input);
                parameters.Add("@IsAvailable", isAvailable, DbType.Boolean, ParameterDirection.Input);

                await GetAsync<DeliveryPartner>("markPartnerActive", parameters, CommandType.StoredProcedure);
            }
            catch (Exception ex)
            {
                throw new Exception("Internal Server Error", ex);
            }

        }

    }
}
