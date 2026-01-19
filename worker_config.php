<?php
// worker_config.php - Configuration for workers

return [
    'concurrency' => [
        'max_workers' => 16,
        'batch_size' => 1000,
        'timeout_per_request' => 30,
        'max_retries' => 3,
        'delay_between_requests' => 50, // milliseconds
    ],
    
    'networking' => [
        'max_connections' => 50,
        'connection_timeout' => 10,
        'request_timeout' => 30,
        'user_agent' => 'Mozilla/5.0 (compatible; ParallelScraper/2.0)',
        'accept_encoding' => 'gzip,deflate',
    ],
    
    'memory' => [
        'memory_limit' => '512M', // per worker
        'max_batch_memory' => 1000000, // bytes
    ],
    
    'logging' => [
        'enabled' => true,
        'level' => 'INFO', // DEBUG, INFO, WARNING, ERROR
        'log_file' => 'scraper_worker.log',
        'error_file' => 'scraper_errors.log',
    ],
    
    'performance' => [
        'use_gzip' => true,
        'cache_responses' => false,
        'respect_robots_txt' => false,
        'delay_between_batches' => 100, // milliseconds
    ]
];