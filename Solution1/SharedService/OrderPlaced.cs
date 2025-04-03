using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SharedService
{
public sealed record OrderPlaced(
        [property: BsonId, BsonRepresentation(BsonType.String)] Guid OrderId,
        int RestaurantId,
        string RestaurantName,
        int ItemId,
        string ItemName,
        string ItemDescription
    );}
