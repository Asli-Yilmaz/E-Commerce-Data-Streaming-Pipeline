using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Poducer
{
    public class OrderEvent
    {
        public Guid OrderId { get; set; }
        public string CustomerName { get; set; }
        public string Product { get; set; }
        public decimal Price { get; set; }
        public string City { get; set; } 
        public DateTime OrderDate { get; set; }

    }
}
