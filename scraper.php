<?php
// scraper.php

ini_set('memory_limit', '1024M');
set_time_limit(0);

define('FTP_HOST', getenv('FTP_HOST'));
define('FTP_USER', getenv('FTP_USER'));
define('FTP_PASS', getenv('FTP_PASS'));
define('FTP_BASE_DIR', getenv('FTP_BASE_DIR'));
define('CURR_URL', getenv('CURR_URL'));

const SITEMAP_INDEX = CURR_URL.'/sitemap.xml';
const OUTPUT_CSV    = 'products_full.csv';

/* ---------------- FTP ---------------- */

function uploadToFtp(string $file): void
{
    $conn = ftp_connect(FTP_HOST, 21, 30);
    ftp_login($conn, FTP_USER, FTP_PASS);
    ftp_pasv($conn, true);
    ensureFtpDir($conn, FTP_BASE_DIR);
    ftp_chdir($conn, FTP_BASE_DIR);
    ftp_put($conn, basename($file), $file, FTP_BINARY);
    ftp_close($conn);
}

function ensureFtpDir($conn, string $path): void
{
    foreach (explode('/', trim($path, '/')) as $dir) {
        if (!@ftp_chdir($conn, $dir)) {
            ftp_mkdir($conn, $dir);
            ftp_chdir($conn, $dir);
        }
    }
}

/* ---------------- HTTP ---------------- */

function httpGet(string $url): ?string
{
    return @file_get_contents($url, false, stream_context_create([
        'http' => ['timeout' => 30, 'user_agent' => 'EE-Scraper/1.0']
    ])) ?: null;
}

function loadXml(string $url): ?SimpleXMLElement
{
    $xml = httpGet($url);
    return $xml ? simplexml_load_string($xml) : null;
}

function fetchJson(string $url): ?array
{
    $json = httpGet($url);
    return $json ? json_decode($json, true) : null;
}

function normalizeImage(string $url): string
{
    return strpos($url, '//') === 0 ? 'https:' . $url : $url;
}

/* ---------------- Product ---------------- */

function processProduct(string $url, $csv, array &$seen): void
{
    if (isset($seen[$url])) return;
    $seen[$url] = true;

    $product = fetchJson(rtrim($url, '/') . '.js');
    if (!$product || empty($product['variants'])) return;

    $options  = $product['options'] ?? [];
    $images   = implode(',', array_map('normalizeImage', $product['images'] ?? []));

    foreach ($product['variants'] as $v) {
        fputcsv($csv, [
            $product['id'],
            $product['title'],
            $product['vendor'],
            $product['type'],
            $product['handle'],
            $v['id'],
            $v['title'],
            $v['sku'] ?? '',
            $options[0]['name'] ?? '',
            $v['option1'] ?? '',
            $options[1]['name'] ?? '',
            $v['option2'] ?? '',
            $options[2]['name'] ?? '',
            $v['option3'] ?? '',
            $v['price'],
            $v['available'] ? '1' : '0',
            rtrim($url, '/') . '?variant=' . $v['id'],
            $images
        ]);
    }

    usleep(150000);
}

/* ---------------- Main ---------------- */

$index = loadXml(SITEMAP_INDEX);
$index->registerXPathNamespace('ns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
$sitemaps = $index->xpath('//ns:sitemap/ns:loc');

$csv = fopen(OUTPUT_CSV, 'w');
fputcsv($csv, [
    'product_id','product_title','vendor','type','handle',
    'variant_id','variant_title','sku',
    'option_1_name','option_1_value',
    'option_2_name','option_2_value',
    'option_3_name','option_3_value',
    'variant_price','available','variant_url','image_url'
]);

$seen = [];

foreach ($sitemaps as $map) {
    $xml = loadXml((string)$map);
    $xml->registerXPathNamespace('ns', $xml->getNamespaces(true)['']);
    foreach ($xml->xpath('//ns:url/ns:loc') as $loc) {
        processProduct((string)$loc, $csv, $seen);
    }
}

fclose($csv);
uploadToFtp(OUTPUT_CSV);