using Bogus;
using Confluent.Kafka;
using System.Collections.Concurrent;
using System.Text.Json;

namespace Poducer
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly Faker<OrderEvent> _orderFaker;
        private readonly IProducer<Null, string> _producer;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
            var cities = new[] { "Adana", "Adiyaman", "Afyon", "Agri", "Aksaray", "Amasya", "Ankara", "Antalya", "Ardahan", "Artvin", "Aydin", "Balikesir", "Bartin", "Batman", "Bayburt", "Bilecik", "Bingol", "Bitlis", "Bolu", "Burdur", "Bursa", "Canakkale", "Cankiri", "Corum", "Denizli", "Diyarbakir", "Duzce", "Edirne", "Elazig", "Erzincan", "Erzurum", "Eskisehir", "Gaziantep", "Giresun", "Gumushane", "Hakkari", "Hatay", "Igdir", "Isparta", "Istanbul", "Izmir", "Kahramanmaras", "Karabuk", "Karaman", "Kars", "Kastamonu", "Kayseri", "Kilis", "Kirikkale", "Kirklareli", "Kirsehir", "Kocaeli", "Konya", "Kutahya", "Malatya", "Manisa", "Mardin", "Mersin", "Mugla", "Mus", "Nevsehir", "Nigde", "Ordu", "Osmaniye", "Rize", "Sakarya", "Samsun", "Sanliurfa", "Siirt", "Sinop", "Sirnak", "Sivas", "Tekirdag", "Tokat", "Trabzon", "Tunceli", "Usak", "Van", "Yalova", "Yozgat", "Zonguldak" };
            //kafka ayarlarý
            var config = new ProducerConfig { 
                BootstrapServers = "ecommerce-stream-asliyilmaz613-776b.j.aivencloud.com:14378",
                SecurityProtocol = SecurityProtocol.Ssl,
                SslCaLocation = "ca.pem",
                SslCertificateLocation = "service.cert",
                SslKeyLocation = "service.key",
                MessageTimeoutMs = 5000
            };
            _producer = new ProducerBuilder<Null,string>(config).Build();

            
            _orderFaker = new Faker<OrderEvent>()
                .RuleFor(o => o.OrderId, f => Guid.NewGuid())
                .RuleFor(o => o.CustomerName, f => f.Name.FullName())
                .RuleFor(o => o.Product, f => f.Commerce.ProductName())
                .RuleFor(o => o.Price, f => decimal.Parse(f.Commerce.Price(10,100000)))
                .RuleFor(o => o.City, f => f.PickRandom(cities))
                .RuleFor(o => o.OrderDate, f => DateTime.UtcNow);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                
                var order = _orderFaker.Generate();
                var message=JsonSerializer.Serialize(order);

                try
                {
                    //topic'e veri gonderilmesi
                    await _producer.ProduceAsync("ecommerce-data", new Message<Null, string> { Value = message }, stoppingToken);
                    _logger.LogInformation($"order send: {order.City} - {order.Price} TL");
                }
                catch (Exception ex) 
                {
                    _logger.LogError($"error : {ex.Message}");
                }
                
                //stream simulasyonu
                await Task.Delay(1000,stoppingToken);
            }
        }
    }
}
