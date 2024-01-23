<?php
require_once "config.php";

set_time_limit(24 * 60 * 60);
ini_set("memory_limit", "2G");

$timestamp = $_POST["timestamp"];

$format = "Y-m-d\TH:i";
$d = DateTime::createFromFormat($format, $timestamp);
if($d && $d->format($format) == $timestamp && $d < new DateTime()) {
    $dsn = "pgsql:host=" . DB_HOST . ";port=5432;dbname=" . DB_NAME . ";";
    $pdo = new PDO($dsn, DB_USER, DB_PASS, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
    ]);
    $timestampoutput = $d->format("U") . "000";

    $stmt = $pdo->prepare("SELECT * FROM objects WHERE visibleOn <= :t AND (disappearedOn >= :t OR disappearedOn IS NULL)");
    $stmt->execute(["t" => $timestampoutput]);

    $path = "/var/www/html/output/rpki-snapshot-" . uniqid() . ".zip";
    register_shutdown_function('unlink', $path);

    $a = new ZipArchive();
    $a->open($path, ZipArchive::CREATE);
    while($data = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $filepath = str_replace("rsync://", "", $data["uri"]);
        $a->addFromString($filepath, base64_decode($data["content"]));
        // $a->setMtimeName($filepath, (int)substr($data["visibleOn"], 0, -3));
    }
    $a->close();

    // shell_exec("python3 /home/ubuntu/delta/reconstruct.py /home/ubuntu/delta/rpki.db /home/ubuntu/delta/app.properties " . escapeshellarg($timestamp . " UTC") . " " . escapeshellarg($path));
    header("Content-Type: application/zip");
    header("Content-Length: " . filesize($path));
    header("Content-Disposition: attachment; filename=\"rpki.zip\"");
    readfile($path);
    unset($a);
    unlink($path);
} else {
    echo "Invalid date";
}