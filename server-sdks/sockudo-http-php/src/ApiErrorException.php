<?php

namespace Sockudo;

/**
 * HTTP error responses.
 * getCode() will return the response HTTP status code,
 * and getMessage() will return the response body.
 */
class ApiErrorException extends SockudoException
{
    /**
     * Returns the string representation of the exception.
     *
     * @return string
     */
    public function __toString(): string
    {
        return "(Status {$this->getCode()}) {$this->getMessage()}";
    }
}
