using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace FoodDelivery.APIGateway.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AuthContoller : ControllerBase
    {
        private readonly IConfiguration _configuration;

        public AuthContoller(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        [HttpPost("token")]
        public IActionResult GenerateToken()
        {
            var claims = new[]
            {
            new Claim(JwtRegisteredClaimNames.Sub, "user1"),
            new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString())
        };

            var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_configuration["Jwt:SecretKey"]));
            var credentials = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);
            var token = new JwtSecurityToken(
                _configuration["Jwt:Issuer"],
                _configuration["Jwt:Audience"],
                claims,
                expires: DateTime.UtcNow.AddHours(1),
                signingCredentials: credentials);

            return Ok(new { token = new JwtSecurityTokenHandler().WriteToken(token) });
        }
    }
}
