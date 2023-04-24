<?php
$timestamp = $_POST["timestamp"];

$format = "Y-m-d\TH:i";
$d = DateTime::createFromFormat($format, $timestamp);
if($d && $d->format($format) == $timestamp && $d < new DateTime()) {
    $timestampoutput = $d->format("U") . "0000";
    $path = "/var/www/html/output/rpki-snapshot-$timestampoutput";
    shell_exec("python3 /opt/reconstruct.py /opt/rpki.db " . escapeshellarg($timestamp . " UTC") . " " . escapeshellarg($path));
    header("Location: output/rpki-snapshot-$timestampoutput.tar.gz");
} else {
    echo "Invalid date";
}