namespace Order.WebApi.Models
{
    
    public record OrderRequest(Guid OrderId, int RestaurantId, string RestaurantName, int ItemId, string ItemName, string ItemDescription);
}
