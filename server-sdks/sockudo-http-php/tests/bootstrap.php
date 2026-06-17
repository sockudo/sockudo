<?php

error_reporting(E_ALL);

$vendorAutoload = dirname(__DIR__) . "/vendor/autoload.php";
if (file_exists($vendorAutoload)) {
    require_once $vendorAutoload;
}

$srcFiles = [
    dirname(__DIR__) . "/src/SockudoInterface.php",
    dirname(__DIR__) . "/src/SockudoException.php",
    dirname(__DIR__) . "/src/ApiErrorException.php",
    dirname(__DIR__) . "/src/SockudoCrypto.php",
    dirname(__DIR__) . "/src/Webhook.php",
    dirname(__DIR__) . "/src/SockudoInstance.php",
    dirname(__DIR__) . "/src/Sockudo.php",
];

foreach ($srcFiles as $srcFile) {
    if (file_exists($srcFile)) {
        require_once $srcFile;
    }
}

if (file_exists(__DIR__ . "/config.php") === true) {
    require "config.php";
} else {
    define("SOCKUDOAPP_AUTHKEY", getenv("SOCKUDOAPP_AUTHKEY") ?: '');
    define("SOCKUDOAPP_SECRET", getenv("SOCKUDOAPP_SECRET") ?: '');
    define("SOCKUDOAPP_APPID", getenv("SOCKUDOAPP_APPID") ?: '');

    define("SOCKUDOAPP_CLUSTER", getenv("SOCKUDOAPP_CLUSTER") ?: '');

    define("TEST_CHANNEL", getenv("TEST_CHANNEL") ?: '');
}
