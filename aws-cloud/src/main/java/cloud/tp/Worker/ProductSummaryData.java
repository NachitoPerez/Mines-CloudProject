package cloud.tp.Worker;

public class ProductSummaryData {
        private int totalQuantity;
        private double totalSold;
        private double totalProfit;

        public ProductSummaryData(int totalQuantity, double totalSold, double totalProfit) {
            this.totalQuantity = totalQuantity;
            this.totalSold = totalSold;
            this.totalProfit = totalProfit;
        }

        public void update(int quantity, double sold, double profit) {
            this.totalQuantity += quantity;
            this.totalSold += sold;
            this.totalProfit += profit;
        }

        public int getTotalQuantity() {
            return totalQuantity;
        }

        public double getTotalSold() {
            return totalSold;
        }

        public double getTotalProfit() {
            return totalProfit;
        }
    }
