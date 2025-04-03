using Restaurant.WebApi.IRepository;

namespace Restaurant.WebApi.Repository
{
    public class ResturantRepository(IConfiguration configuration):BaseRepository(configuration),IRestaurantRepository
    {
        protected readonly new IConfiguration _configuration = configuration;

    }
}
