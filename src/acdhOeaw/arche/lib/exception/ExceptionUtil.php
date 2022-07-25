<?php

/*
 * The MIT License
 *
 * Copyright 2022 zozlak.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

namespace acdhOeaw\arche\lib\exception;

use Throwable;
use GuzzleHttp\Exception\RequestException;

/**
 * Utility class for unwrapping errors
 *
 * @author zozlak
 */
class ExceptionUtil {

    static public function unwrap(Throwable $e, bool $includeBacktrace = false): string {
        while ($e->getPrevious()) {
            $e = $e->getPrevious();
        }
        if ($e instanceof RequestException && $e->hasResponse()) {
            $resp = $e->getResponse();
            $msg  = "HTTP " . $resp?->getStatusCode() . " with message: " . $resp?->getBody() . "\n";
        } else {
            $msg = $e->getMessage();
        }
        if ($includeBacktrace) {
            $msg .= "--------------------\n--- Exception backtrace:\n";
            $msg .= print_r($e, true);
        }
        return $msg;
    }
}
