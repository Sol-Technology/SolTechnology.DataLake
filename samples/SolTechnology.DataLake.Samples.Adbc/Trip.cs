using System.Runtime.Serialization;
using SolTechnology.Time;

namespace SolTechnology.DataLake.Samples.Adbc;

/// <summary>A row of <c>samples.nyctaxi.trips</c>, the sample table every Databricks workspace has.</summary>
public sealed class Trip
{
    [DataMember(Name = "tpep_pickup_datetime")] public DateTimeZone PickedUp { get; set; }
    [DataMember(Name = "tpep_dropoff_datetime")] public DateTimeZone DroppedOff { get; set; }
    [DataMember(Name = "trip_distance")] public double Distance { get; set; }
    [DataMember(Name = "fare_amount")] public double Fare { get; set; }
    [DataMember(Name = "pickup_zip")] public int? PickupZip { get; set; }
    [DataMember(Name = "dropoff_zip")] public int? DropoffZip { get; set; }
}
