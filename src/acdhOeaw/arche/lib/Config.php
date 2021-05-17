<?php

/*
 * The MIT License
 *
 * Copyright 2021 zozlak.
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

namespace acdhOeaw\arche\lib;

use acdhOeaw\arche\lib\exception\RepoLibException;
use function GuzzleHttp\json_decode;
use function GuzzleHttp\json_encode;

/**
 * A container for the yaml configuration allowing to satisfy phpstan checks
 *
 * @author zozlak
 * @property Config $auth
 * @property Config $rest
 * @property Config $transactionController
 * @property Config $metadataManagment
 * @property Config $httpBasic
 * @property Config $headers
 * @property object $schema
 * @property array<string> $nonRelationProperties
 * @property array<string> $options
 * @property int $timeout
 * @property string $pathBase
 * @property string $urlBase
 * @property string $dbConnStr
 * @property object $httpHeader
 * @property string $user;
 * @property string $password
 * @property string $metadataReadMode
 * @property bool $verifyCert
 */
class Config {

    private object $cfg;

    public function __construct(string | object $config) {
        if (is_string($config)) {
            $this->cfg = (object) json_decode(json_encode(yaml_parse_file($config)));
        } else {
            $this->cfg = $config;
        }
    }

    public function __get(string $name): mixed {
        return $this->cfg->$name ?? throw new RepoLibException("Unknown configuration property $name");
    }

    public function asObject(): object {
        return $this->cfg;
    }
    
    public function asYaml(): string {
        return yaml_emit(json_decode(json_encode($this->cfg), true));
    }
}
