<?php
// monitor_scraper.php - Monitor scraper progress

class ScraperMonitor {
    private $logFile = 'scraper.log';
    private $startTime;
    
    public function __construct() {
        $this->startTime = time();
    }
    
    public function displayProgress(): void {
        clearstatcache();
        
        if (!file_exists($this->logFile)) {
            echo "Log file not found.\n";
            return;
        }
        
        $logContent = file_get_contents($this->logFile);
        $lines = explode("\n", $logContent);
        
        $stats = [
            'batches_completed' => 0,
            'products_processed' => 0,
            'errors' => 0,
            'last_activity' => 'None'
        ];
        
        foreach ($lines as $line) {
            if (strpos($line, 'Batch') !== false && strpos($line, 'completed') !== false) {
                $stats['batches_completed']++;
                
                // Extract product count
                if (preg_match('/with (\d+) products/', $line, $matches)) {
                    $stats['products_processed'] += (int)$matches[1];
                }
            }
            
            if (strpos($line, 'ERROR') !== false) {
                $stats['errors']++;
            }
            
            if (trim($line)) {
                $stats['last_activity'] = substr($line, 0, 100);
            }
        }
        
        $elapsed = time() - $this->startTime;
        $productsPerSecond = $elapsed > 0 ? $stats['products_processed'] / $elapsed : 0;
        
        echo "========================================\n";
        echo "SCRAPER MONITOR\n";
        echo "========================================\n";
        echo "Elapsed Time: " . gmdate("H:i:s", $elapsed) . "\n";
        echo "Batches Completed: " . $stats['batches_completed'] . "\n";
        echo "Products Processed: " . number_format($stats['products_processed']) . "\n";
        echo "Processing Rate: " . number_format($productsPerSecond, 2) . " products/sec\n";
        echo "Errors: " . $stats['errors'] . "\n";
        echo "Last Activity: " . $stats['last_activity'] . "\n";
        
        if (file_exists('products_full.csv')) {
            $size = filesize('products_full.csv');
            echo "Output File Size: " . $this->formatBytes($size) . "\n";
        }
        
        echo "========================================\n";
    }
    
    private function formatBytes($bytes): string {
        $units = ['B', 'KB', 'MB', 'GB', 'TB'];
        $i = 0;
        while ($bytes >= 1024 && $i < count($units) - 1) {
            $bytes /= 1024;
            $i++;
        }
        return round($bytes, 2) . ' ' . $units[$i];
    }
}

// Run monitor
if (php_sapi_name() === 'cli') {
    $monitor = new ScraperMonitor();
    
    echo "Starting scraper monitor. Press Ctrl+C to stop.\n\n";
    
    while (true) {
        $monitor->displayProgress();
        sleep(5); // Update every 5 seconds
        echo "\033[" . (15) . "A"; // Move cursor up
    }
}