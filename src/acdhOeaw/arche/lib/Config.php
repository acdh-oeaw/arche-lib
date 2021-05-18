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
 * by mocking config properties hierarchy.
 *
 * @author zozlak
 * @property Config $auth
 * @property Config $rest
 * @property Config $transactionController
 * @property Config $metadataManagment
 * @property Config $httpBasic
 * @property Config $headers
 * @property Config $storage
 * @property Config $logging
 * @property Config $storage
 * @property Config $logging
 * @property Config $accessControl
 * @property Config $socket
 * @property Config $metadataManager
 * @property Config $db
 * @property Config $autoAddIds
 * @property Config $spatialSearch
 * @property Config $create
 * @property Config $fullTextSearch
 * @property Config $schema
 * @property Config | null $rabbitMq
 * @property Config $sizeLimits
 * @property Config $mimeFilter
 * @property Config $handlers
 * @property Config $dbConn
 * @property object $httpHeader
 * @property string $pathBase
 * @property string $urlBase
 * @property string $user;
 * @property string $password
 * @property string $metadataReadMode
 * @property string $dir
 * @property string $dbConnStr
 * @property string $tmpDir
 * @property string $file
 * @property string $address
 * @property string $path
 * @property string $parent
 * @property string $id
 * @property string $searchMatch
 * @property string $label
 * @property string $delete
 * @property string $creationDate
 * @property string $modificationDate
 * @property string $creationUser
 * @property string $metadataWriteMode
 * @property string $modificationUser
 * @property string $publicRole
 * @property string $binaryModificationUser
 * @property string $binaryModificationDate
 * @property string $type
 * @property string $defaultMetadataSearchMode
 * @property string $defaultMetadataFormat
 * @property string $defaultMetadataReadMode
 * @property string $defaultMetadataWriteMode
 * @property string $connStr
 * @property string $dataCol
 * @property string $table
 * @property string $userCol
 * @property string $admin
 * @property string $guest
 * @property string $class
 * @property string $defaultAction
 * @property string $hashAlgorithm
 * @property string $hash
 * @property string $defaultMime
 * @property string $modeDir
 * @property string $level
 * @property string $sizeLimit
 * @property string $tikaLocation
 * @property string $cors
 * @property string $lang
 * @property string $fileName
 * @property string $binarySize
 * @property string $value
 * @property string $indexing
 * @property string $host
 * @property string $read
 * @property string $queue
 * @property string $function
 * @property string $highlighting
 * @property string $transactionId
 * @property array $mime
 * @property array<string> $nonRelationProperties
 * @property array<string> $options
 * @property array<string, array<Config>> $fixed
 * @property array<string, array<Config>> $default
 * @property array $forbidden
 * @property array $namespaces
 * @property array $copying
 * @property array $parameters
 * @property array $metadataFormats
 * @property array $skipNamespaces
 * @property array $addNamespaces
 * @property array $denyNamespaces
 * @property array $properties
 * @property array $adminRoles
 * @property array $allowedRoles
 * @property array $assignRoles
 * @property array $creatorRights
 * @property array $mimeTypes
 * @property array $methods
 * @property array<string, string> $classLoader
 * @property array<Config> $authMethods
 * @property int $timeout
 * @property int $port
 * @property int $levels
 * @property int $checkInterval
 * @property bool $exceptionOnTimeout
 * @property bool $verifyCert
 * @property bool $enforceOnMetadata
 * @property bool $enforceCompleteness
 * @property bool $simplifyMetaHistory
 */
class Config {

    static public function fromYaml(string $path): Config {
        return new self((object) json_decode(json_encode(yaml_parse_file($path))));
    }

    private object $cfg;

    public function __construct(object $config) {
        if ($config instanceof Config) {
            $this->cfg = $config->cfg;
        } else {
            $this->cfg = $config;
        }
    }

    public function __get(string $name): mixed {
        return $this->cfg->$name ?? throw new RepoLibException("Unknown configuration property $name");
    }

    public function __isset(string $name): bool {
        return isset($this->cfg->$name);
    }
    
    public function asObject(): object {
        return $this->cfg;
    }

    /**
     * 
     * @return array<mixed>
     */
    public function asArray(): array {
        return (array) json_decode(json_encode($this->cfg), true);
    }

    public function asYaml(): string {
        return yaml_emit($this->asArray());
    }
}
