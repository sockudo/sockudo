<?php

namespace Sockudo;

class SockudoInstance
{
    private static $instance = null;
    private static $app_id = '';
    private static $secret = '';
    private static $api_key = '';

    /**
     * Get the sockudo singleton instance.
     *
     * @return Sockudo
     * @throws SockudoException
     */
    public static function get_sockudo()
    {
        if (self::$instance !== null) {
            return self::$instance;
        }

        self::$instance = new Sockudo(
            self::$api_key,
            self::$secret,
            self::$app_id
        );

        return self::$instance;
    }
}
