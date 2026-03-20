using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using Microsoft.Data.SqlClient;
using Dapper;
using System.Text.Json;

namespace ConsoleApp1
{
    public class Buyer
    {
        public int Id { get; set; }
        public string Name { get; set; } = null!;
        public string BirthDate { get; set; } = null!;
        public string Sex { get; set; } = null!;
        public string Email { get; set; } = null!;
        public string Country { get; set; } = null!;
        public string City { get; set; } = null!;
        
        public string? Interests { get; set; } 
        public List<string> InterestsList => string.IsNullOrEmpty(Interests) 
            ? new List<string>() 
            : JsonSerializer.Deserialize<List<string>>(Interests) ?? new List<string>();
    }

    public class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = null!;
        public string Country { get; set; } = null!;
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
    }

    class Program
    {
        private const string ConnectionString = @"Server=RaductionPc\Test;Database=Clients&Sales;Trusted_Connection=True;TrustServerCertificate=True";

        static async Task Main(string[] args)
        {
            while (true)
            {
                Console.WriteLine("\n--- MENU (DapperProgram Version) ---");
                Console.WriteLine("1. Count of buyers in all cities");
                Console.WriteLine("2. Count of buyers in all countries");
                Console.WriteLine("3. Count of cities in all countries");
                Console.WriteLine("4. AVG count of cities in all countries");
                Console.WriteLine("5. Show buyers' interests in Ukraine");
                Console.WriteLine("6. Show sale products of PCs from 12.04.2020 to 18.12.2025");
                Console.WriteLine("7. Show all Tom's sale products ");
                Console.WriteLine("8. Show top 3 countries by count of buyers");
                Console.WriteLine("9. Show the best country");
                Console.WriteLine("10. Show top 3 cities by count of buyers");
                Console.WriteLine("11. Show the best city");
                Console.WriteLine("0. Exit");

                string choice = Console.ReadLine() ?? "";

                switch (choice)
                {
                    case "1":
                        using (IDbConnection db = new SqlConnection(ConnectionString))
                        {
                            string sql1 = @"SELECT City, COUNT(*) AS BuyerCount FROM Buyers GROUP BY City";
                            var results = (await db.QueryAsync(sql1)).ToList();
                            Console.WriteLine("Кількість покупців у містах:");
                            foreach (var item in results)
                            {
                                Console.WriteLine($"{item.City}, {item.BuyerCount}");
                            }
                        }
                        break;

                    case "2":
                        using (IDbConnection db = new SqlConnection(ConnectionString))
                        {
                            string sql2 = @"SELECT Country, COUNT(*) AS BuyerCount FROM Buyers GROUP BY Country";
                            var results = (await db.QueryAsync(sql2)).ToList();
                            Console.WriteLine("Кількість покупців у країнах:");
                            foreach (var item in results)
                            {
                                Console.WriteLine($"{item.Country}, {item.BuyerCount}");
                            }
                        }
                        break;

                    case "3":
                        using (IDbConnection db = new SqlConnection(ConnectionString))
                        {
                            string sql3 = @"SELECT Country, COUNT(DISTINCT City) AS CityCount FROM Buyers GROUP BY Country";
                            var results = (await db.QueryAsync(sql3)).ToList();
                            Console.WriteLine("Кількість міст у країнах:");
                            foreach (var item in results)
                            {
                                Console.WriteLine($"{item.Country}, {item.CityCount}");
                            }
                        }
                        break;

                    case "4":
                        using (IDbConnection db = new SqlConnection(ConnectionString))
                        {
                            string sql4 = @"SELECT AVG(CAST(CityCount AS FLOAT)) FROM (SELECT COUNT(DISTINCT City) AS CityCount FROM Buyers GROUP BY Country) AS Sub";
                            double averageCities = await db.ExecuteScalarAsync<double>(sql4);
                            Console.WriteLine($"AVG: {averageCities:F2}");
                        }
                        break;

                    case "5":
                        using (IDbConnection db = new SqlConnection(ConnectionString))
                        {
                            string sql5 = @"SELECT Interests FROM Buyers WHERE Country = 'Ukraine'";
                            var results = await db.QueryAsync<string>(sql5);
                            var interests = results
                                .Where(js => !string.IsNullOrEmpty(js))
                                .SelectMany(js => JsonSerializer.Deserialize<List<string>>(js) ?? new List<string>())
                                .Distinct()
                                .ToList();

                            Console.WriteLine("Інтереси покупців:");
                            foreach (var interest in interests)
                            {
                                Console.WriteLine($"{interest}");
                            }
                        }
                        break;

                    case "6":
                        using (IDbConnection db = new SqlConnection(ConnectionString))
                        {
                            string sql6 = @"SELECT * FROM Products WHERE Name = 'PC' AND Start > '2020-04-12' AND [End] < '2025-12-18'";
                            var products = (await db.QueryAsync<Product>(sql6)).ToList();
                            
                            Console.WriteLine("Товари:");
                            foreach (var product in products)
                            {
                                Console.WriteLine($"{product.Name} ({product.Start:d} - {product.End:d})");
                            }
                        }
                        break;
                    case "7":
                        using (IDbConnection db = new SqlConnection(ConnectionString)){
                        string sql7 = @"SELECT p.* FROM Products p
                                        JOIN BuyerProduct bp ON p.Id = bp.ProductsId
                                        JOIN Buyers b ON b.Id = bp.BuyersId
                                        WHERE b.Name = 'Tom'";
                        
                        var saleProducts =  (await db.QueryAsync<Product>(sql7)).ToList();
                        Console.WriteLine("Товари:");
                        foreach (var product in saleProducts)
                        {
                            Console.WriteLine($"{product.Name} ({product.Start:d} - {product.End:d})");
                        }
                        }
                        break;
                    case "8":
                        using (IDbConnection db = new SqlConnection(ConnectionString))
                        {
                            string sql8 = @"SELECT TOP 3 Country, COUNT(*) AS BuyerCount 
                                            FROM Buyers GROUP BY Country 
                                            ORDER BY BuyerCount DESC";
                            var results = (await db.QueryAsync(sql8)).ToList();
                            Console.WriteLine("Топ-3 країни за кількістю покупців:");
                            foreach (var item in results)
                            {
                                Console.WriteLine($"{item.Country}: {item.BuyerCount}");
                            }
                        }
                        break;

                    case "9":
                        using (IDbConnection db = new SqlConnection(ConnectionString))
                        {
                            string sql9 = @"SELECT TOP 1 Country, COUNT(*) AS BuyerCount 
                                            FROM Buyers GROUP BY Country 
                                            ORDER BY BuyerCount DESC";
                            var result = await db.QueryFirstOrDefaultAsync(sql9);
                            if (result != null)
                                Console.WriteLine($"Найкраща країна: {result.Country} ({result.BuyerCount} покупців)");
                        }
                        break;

                    case "10":
                        using (IDbConnection db = new SqlConnection(ConnectionString))
                        {
                            string sql10 = @"SELECT TOP 3 City, COUNT(*) AS BuyerCount 
                                             FROM Buyers GROUP BY City 
                                             ORDER BY BuyerCount DESC";
                            var results = (await db.QueryAsync(sql10)).ToList();
                            Console.WriteLine("Топ-3 міста за кількістю покупців:");
                            foreach (var item in results)
                            {
                                Console.WriteLine($"{item.City}: {item.BuyerCount}");
                            }
                        }
                        break;

                    case "11":
                        using (IDbConnection db = new SqlConnection(ConnectionString))
                        {
                            string sql11 = @"SELECT TOP 1 City, COUNT(*) AS BuyerCount 
                                             FROM Buyers GROUP BY City 
                                             ORDER BY BuyerCount DESC";
                            var result = await db.QueryFirstOrDefaultAsync(sql11);
                            if (result != null)
                                Console.WriteLine($"Найкраще місто: {result.City} ({result.BuyerCount} покупців)");
                        }
                        break;

                    case "0":
                        return;

                    default:
                        Console.WriteLine("Invalid choice.");
                        break;
                }
            }
        }
    }
}