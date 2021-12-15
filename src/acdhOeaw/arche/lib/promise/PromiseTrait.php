<?php

/*
 * The MIT License
 *
 * Copyright 2021 Austrian Centre for Digital Humanities.
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

namespace acdhOeaw\arche\lib\promise;

use GuzzleHttp\Promise\PromiseInterface;

/**
 * Description of PromiseTrait
 *
 * @author zozlak
 */
trait PromiseTrait {

    // debug stuff
    static private int $__count = 0;
    static public bool $debug    = false;
    private string $caller;
    private int $id;
    // actually needed
    private PromiseInterface $promise;

    public function __construct(PromiseInterface $promise) {
        $this->promise = $promise;
        if (self::$debug) {
            self::$__count++;
            $this->id     = self::$__count;
            $caller       = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 3);
            $this->caller = ($caller[1]['class'] ?? '') . '::' . ($caller[1]['function'] ?? '') . '():' . ($caller[0]['line'] ?? '') .
                ',' . ($caller[2]['class'] ?? '') . '::' . ($caller[2]['function'] ?? '') . '():' . ($caller[1]['line'] ?? '');
            echo "RepoResourceTrait [$this->id] created from " . get_class($promise) . " caller $this->caller\n";
        }
    }

    public function cancel(): void {
        $this->promise->cancel();
    }

    public function getState(): string {
        return $this->promise->getState();
    }

    public function otherwise(callable $onRejected): PromiseInterface {
        return $this->promise->otherwise($onRejected);
    }

    public function then(callable $onFulfilled = null,
                         callable $onRejected = null): PromiseInterface {
        return $this->promise->then($onFulfilled, $onRejected);
    }
}
