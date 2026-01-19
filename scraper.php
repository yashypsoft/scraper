<?php
// parallel_scraper.php

require __DIR__ . '/vendor/autoload.php';

use Amp\Parallel\Worker;
use Amp\Future;
use Amp\Sync\Channel;
use League\Csv\Writer;
use League\Csv\Reader;

ini_set('memory_limit', '4096M');
set_time_limit(0);
error_reporting(E_ALL);
ini_set('display_errors', 1);
ini_set('log_errors', 1);
ini_set('error_log', 'scraper_errors.log');

// Configuration
define('FTP_HOST', getenv('FTP_HOST'));
define('FTP_USER', getenv('FTP_USER'));
define('FTP_PASS', getenv('FTP_PASS'));
define('FTP_BASE_DIR', getenv('FTP_BASE_DIR') ?: '/');
define('CURR_URL', rtrim(getenv('CURR_URL'), '/'));
define('MAX_WORKERS', (int) (getenv('MAX_WORKERS') ?: 8));
define('BATCH_SIZE', (int) (getenv('BATCH_SIZE') ?: 500));
define('REQUEST_TIMEOUT', 30);
define('RETRY_ATTEMPTS', 3);
define('RETRY_DELAY', 1000); // milliseconds

const SITEMAP_INDEX = CURR_URL . '/sitemap.xml';
const OUTPUT_CSV = 'products_full.csv';
const TEMP_DIR = 'temp_scrapes';
const URLS_FILE = TEMP_DIR . '/all_urls.txt';
const PROCESSED_FILE = TEMP_DIR . '/processed_urls.txt';

class Logger {
    private static $logFile = 'scraper.log';
    
    public static function log(string $message, string $level = 'INFO'): void {
        $timestamp = date('Y-m-d H:i:s');
        $logMessage = "[$timestamp] [$level] $message\n";
        file_put_contents(self::$logFile, $logMessage, FILE_APPEND);
        echo $logMessage;
    }
    
    public static function error(string $message): void {
        self::log($message, 'ERROR');
    }
    
    public static function info(string $message): void {
        self::log($message, 'INFO');
    }
    
    public static function debug(string $message): void {
        if (getenv('DEBUG')) {
            self::log($message, 'DEBUG');
        }
    }
}

class ProductScraperWorker {
    private $httpClient;
    
    public function __construct() {
        $this->httpClient = new class {
            private $multiHandle;
            private $handles = [];
            
            public function __construct() {
                $this->multiHandle = curl_multi_init();
                curl_multi_setopt($this->multiHandle, CURLMOPT_MAXCONNECTS, 50);
            }
            
            public function addRequest(string $url): int {
                $ch = curl_init();
                curl_setopt_array($ch, [
                    CURLOPT_URL => $url,
                    CURLOPT_RETURNTRANSFER => true,
                    CURLOPT_TIMEOUT => REQUEST_TIMEOUT,
                    CURLOPT_CONNECTTIMEOUT => 10,
                    CURLOPT_FOLLOWLOCATION => true,
                    CURLOPT_MAXREDIRS => 5,
                    CURLOPT_SSL_VERIFYPEER => true,
                    CURLOPT_SSL_VERIFYHOST => 2,
                    CURLOPT_ENCODING => 'gzip,deflate',
                    CURLOPT_USERAGENT => 'Mozilla/5.0 (compatible; ScraperBot/2.0)',
                    CURLOPT_HTTPHEADER => [
                        'Accept: application/json',
                        'Accept-Language: en-US,en;q=0.9',
                        'Cache-Control: no-cache',
                    ],
                ]);
                
                $this->handles[] = $ch;
                curl_multi_add_handle($this->multiHandle, $ch);
                return count($this->handles) - 1;
            }
            
            public function execute(): array {
                $active = null;
                do {
                    $status = curl_multi_exec($this->multiHandle, $active);
                    if ($active) {
                        curl_multi_select($this->multiHandle, 0.5);
                    }
                } while ($active && $status == CURLM_OK);
                
                $results = [];
                foreach ($this->handles as $index => $ch) {
                    $results[$index] = [
                        'content' => curl_multi_getcontent($ch),
                        'error' => curl_error($ch),
                        'code' => curl_getinfo($ch, CURLINFO_HTTP_CODE)
                    ];
                    curl_multi_remove_handle($this->multiHandle, $ch);
                    curl_close($ch);
                }
                
                $this->handles = [];
                return $results;
            }
            
            public function __destruct() {
                curl_multi_close($this->multiHandle);
            }
        };
    }
    
    public function processBatch(array $urls): array {
        Logger::debug("Processing batch of " . count($urls) . " URLs");
        
        $results = [];
        $requests = [];
        
        // Prepare all requests
        foreach ($urls as $index => $url) {
            $jsonUrl = rtrim($url, '/') . '.js';
            $requests[$index] = $this->httpClient->addRequest($jsonUrl);
        }
        
        // Execute all requests in parallel
        $responses = $this->httpClient->execute();
        
        // Process responses
        foreach ($urls as $index => $url) {
            $response = $responses[$requests[$index]] ?? null;
            
            if (!$response || $response['code'] !== 200 || empty($response['content'])) {
                Logger::debug("Failed to fetch: $url (Code: " . ($response['code'] ?? 'N/A') . ")");
                continue;
            }
            
            $product = json_decode($response['content'], true);
            
            if (!$product || !isset($product['variants']) || !is_array($product['variants'])) {
                continue;
            }
            
            $productId = $product['id'] ?? '';
            $productTitle = $product['title'] ?? '';
            $vendor = $product['vendor'] ?? '';
            $type = $product['type'] ?? '';
            $handle = $product['handle'] ?? '';
            
            $images = array_map(function($img) {
                if (strpos($img, '//') === 0) {
                    return 'https:' . $img;
                }
                return $img;
            }, $product['images'] ?? []);
            
            $imageString = implode(',', array_slice($images, 0, 10)); // Limit to 10 images
            
            foreach ($product['variants'] as $variant) {
                $results[] = [
                    'product_id' => $productId,
                    'product_title' => $productTitle,
                    'vendor' => $vendor,
                    'type' => $type,
                    'handle' => $handle,
                    'variant_id' => $variant['id'] ?? '',
                    'variant_title' => $variant['title'] ?? '',
                    'sku' => $variant['sku'] ?? '',
                    'option_1_name' => $product['options'][0]['name'] ?? '',
                    'option_1_value' => $variant['option1'] ?? '',
                    'option_2_name' => $product['options'][1]['name'] ?? '',
                    'option_2_value' => $variant['option2'] ?? '',
                    'option_3_name' => $product['options'][2]['name'] ?? '',
                    'option_3_value' => $variant['option3'] ?? '',
                    'variant_price' => $variant['price'] ?? '0.00',
                    'available' => isset($variant['available']) && $variant['available'] ? '1' : '0',
                    'variant_url' => rtrim($url, '/') . '?variant=' . ($variant['id'] ?? ''),
                    'image_url' => $imageString
                ];
            }
        }
        
        return $results;
    }
}

class BatchProcessor {
    public static function process(string $workerClass, array $batches): array {
        $futures = [];
        
        foreach ($batches as $batchId => $batch) {
            $futures[$batchId] = Worker\submitTask(
                new class($workerClass, $batch) implements Worker\Task {
                    private $workerClass;
                    private $batch;
                    
                    public function __construct(string $workerClass, array $batch) {
                        $this->workerClass = $workerClass;
                        $this->batch = $batch;
                    }
                    
                    public function run(Channel $channel): array {
                        require_once __DIR__ . '/vendor/autoload.php';
                        $scraper = new $this->workerClass();
                        return $scraper->processBatch($this->batch);
                    }
                }
            );
        }
        
        $results = [];
        foreach ($futures as $batchId => $future) {
            try {
                $batchResults = $future->await();
                $results = array_merge($results, $batchResults);
                Logger::info("Batch $batchId completed with " . count($batchResults) . " products");
            } catch (\Throwable $e) {
                Logger::error("Batch $batchId failed: " . $e->getMessage());
            }
        }
        
        return $results;
    }
}

class SitemapParser {
    public static function getAllProductUrls(): array {
        Logger::info("Fetching sitemap index from: " . SITEMAP_INDEX);
        
        $indexContent = @file_get_contents(SITEMAP_INDEX, false, stream_context_create([
            'http' => [
                'timeout' => 60,
                'user_agent' => 'SitemapParser/1.0'
            ]
        ]));
        
        if (!$indexContent) {
            throw new Exception("Failed to fetch sitemap index");
        }
        
        $indexXml = simplexml_load_string($indexContent);
        $indexXml->registerXPathNamespace('ns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
        $sitemapUrls = $indexXml->xpath('//ns:sitemap/ns:loc');
        
        $allUrls = [];
        $urlCount = 0;
        
        foreach ($sitemapUrls as $sitemapUrl) {
            Logger::info("Processing sitemap: " . (string)$sitemapUrl);
            
            $sitemapContent = @file_get_contents((string)$sitemapUrl, false, stream_context_create([
                'http' => ['timeout' => 60]
            ]));
            
            if (!$sitemapContent) {
                Logger::error("Failed to fetch sitemap: " . (string)$sitemapUrl);
                continue;
            }
            
            $sitemapXml = simplexml_load_string($sitemapContent);
            $namespaces = $sitemapXml->getNamespaces(true);
            $namespace = reset($namespaces) ?: '';
            
            if ($namespace) {
                $sitemapXml->registerXPathNamespace('ns', $namespace);
                $urls = $sitemapXml->xpath('//ns:url/ns:loc');
            } else {
                $urls = $sitemapXml->xpath('//url/loc');
            }
            
            foreach ($urls as $url) {
                $urlStr = (string)$url;
                if (strpos($urlStr, '/products/') !== false) {
                    $allUrls[] = $urlStr;
                    $urlCount++;
                    
                    if ($urlCount % 1000 === 0) {
                        Logger::info("Collected $urlCount URLs so far...");
                    }
                }
            }
            
            usleep(100000); // 100ms delay between sitemaps
        }
        
        Logger::info("Total URLs collected: " . count($allUrls));
        return $allUrls;
    }
}

class CSVManager {
    public static function writeProductsToCSV(array $products, string $filename): void {
        if (empty($products)) {
            return;
        }
        
        $fileExists = file_exists($filename);
        $csv = Writer::createFromPath($filename, $fileExists ? 'a' : 'w');
        
        if (!$fileExists) {
            $csv->insertOne([
                'product_id', 'product_title', 'vendor', 'type', 'handle',
                'variant_id', 'variant_title', 'sku',
                'option_1_name', 'option_1_value',
                'option_2_name', 'option_2_value',
                'option_3_name', 'option_3_value',
                'variant_price', 'available', 'variant_url', 'image_url'
            ]);
        }
        
        $csv->insertAll($products);
    }
    
    public static function mergeTempFiles(string $pattern, string $outputFile): void {
        $files = glob($pattern);
        if (empty($files)) {
            return;
        }
        
        $headerWritten = false;
        $output = Writer::createFromPath($outputFile, 'w');
        
        foreach ($files as $file) {
            $reader = Reader::createFromPath($file, 'r');
            $records = $reader->getRecords();
            
            foreach ($records as $index => $record) {
                if (!$headerWritten) {
                    $output->insertOne($record);
                    $headerWritten = true;
                } elseif ($index > 0) {
                    $output->insertOne($record);
                }
            }
            
            unlink($file);
        }
    }
}

class FTPUploader {
    public static function uploadFile(string $file): void {
        Logger::info("Uploading to FTP: " . FTP_HOST . FTP_BASE_DIR);
        
        $conn = ftp_connect(FTP_HOST, 21, 30);
        if (!$conn) {
            throw new Exception("FTP connection failed");
        }
        
        if (!@ftp_login($conn, FTP_USER, FTP_PASS)) {
            throw new Exception("FTP login failed");
        }
        
        ftp_pasv($conn, true);
        
        // Ensure directory exists
        $dirs = explode('/', trim(FTP_BASE_DIR, '/'));
        foreach ($dirs as $dir) {
            if (!empty($dir) && !@ftp_chdir($conn, $dir)) {
                ftp_mkdir($conn, $dir);
                ftp_chdir($conn, $dir);
            }
        }
        
        ftp_chdir($conn, FTP_BASE_DIR);
        
        if (!ftp_put($conn, basename($file), $file, FTP_BINARY)) {
            throw new Exception("FTP upload failed");
        }
        
        ftp_close($conn);
        Logger::info("Upload completed successfully");
    }
}

// Main execution
function main(): void {
    // Create temp directory
    if (!is_dir(TEMP_DIR)) {
        mkdir(TEMP_DIR, 0755, true);
    }
    
    try {
        Logger::info("Starting parallel scraper with " . MAX_WORKERS . " workers");
        Logger::info("Batch size: " . BATCH_SIZE);
        
        // Step 1: Get all product URLs
        Logger::info("Step 1: Fetching product URLs from sitemap");
        $allUrls = SitemapParser::getAllProductUrls();
        
        if (empty($allUrls)) {
            throw new Exception("No product URLs found in sitemap");
        }
        
        Logger::info("Found " . count($allUrls) . " product URLs");
        
        // Step 2: Shuffle URLs to distribute load
        shuffle($allUrls);
        
        // Step 3: Create batches
        $batches = array_chunk($allUrls, BATCH_SIZE);
        Logger::info("Created " . count($batches) . " batches");
        
        // Step 4: Process batches in parallel
        Logger::info("Step 2: Processing batches in parallel");
        
        $tempFiles = [];
        $processedCount = 0;
        
        // Process batches in groups to manage memory
        $batchGroups = array_chunk($batches, MAX_WORKERS * 2);
        
        foreach ($batchGroups as $groupIndex => $batchGroup) {
            Logger::info("Processing batch group " . ($groupIndex + 1) . "/" . count($batchGroups));
            
            $results = BatchProcessor::process(ProductScraperWorker::class, $batchGroup);
            
            // Write results to temporary CSV
            $tempFile = TEMP_DIR . "/products_batch_" . $groupIndex . ".csv";
            CSVManager::writeProductsToCSV($results, $tempFile);
            $tempFiles[] = $tempFile;
            
            $processedCount += count($results);
            Logger::info("Total processed: $processedCount products");
            
            // Clear memory
            unset($results);
            gc_collect_cycles();
        }
        
        // Step 5: Merge all temporary files
        Logger::info("Step 3: Merging CSV files");
        CSVManager::mergeTempFiles(TEMP_DIR . "/products_batch_*.csv", OUTPUT_CSV);
        
        // Step 6: Upload to FTP
        Logger::info("Step 4: Uploading to FTP");
        FTPUploader::uploadFile(OUTPUT_CSV);
        
        // Step 7: Cleanup
        Logger::info("Cleaning up temporary files");
        array_map('unlink', glob(TEMP_DIR . "/*"));
        rmdir(TEMP_DIR);
        
        Logger::info("Scraping completed successfully!");
        Logger::info("Total products processed: $processedCount");
        
    } catch (Exception $e) {
        Logger::error("Fatal error: " . $e->getMessage());
        Logger::error("Stack trace: " . $e->getTraceAsString());
        exit(1);
    }
}

// Run the scraper
main();