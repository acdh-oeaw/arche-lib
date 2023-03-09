<?php

/*
 * The MIT License
 *
 * Copyright 2023 Austrian Centre for Digital Humanities.
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

namespace acdhOeaw\arche\lib\tests;

use PDO;
use acdhOeaw\arche\lib\Config;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\SearchConfig;
use acdhOeaw\arche\lib\SmartSearch;
use zozlak\RdfConstants as RDF;

/**
 * Description of SmartSearchTest
 *
 * @author zozlak
 */
class SmartSearchTest extends \PHPUnit\Framework\TestCase {

    public function testFoo(): void {
        $this->assertTrue(true);
        return;
        
        $log                     = new MyLog();
        $config                  = Config::fromYaml(__DIR__ . '/config.yaml');
        $schema                  = new Schema($config->schema);
        $pdo                     = new PDO('pgsql: host=127.0.0.1 port=5432 user=guest password=-1o6J2sVfMKrwSqEowmSRKHUJmLyOxQ1 dbname=www-data');
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $context                 = [
            $schema->label                                           => 'title',
            RDF::RDF_TYPE                                            => 'class',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasAvailableDate' => 'availableDate',
            $schema->searchFts                                       => 'matchHiglight',
            $schema->searchMatch                                     => 'matchProperty',
            $schema->searchWeight                                    => 'matchWeight',
            $schema->searchOrder                                     => 'matchOrder',
        ];
        $search                  = new SmartSearch($pdo, $schema);
        $search->setPropertyWeights([
            $schema->label                                         => 100.0,
            $schema->id                                            => 50.0,
            'https://vocabs.acdh.oeaw.ac.at/schema#hasDescription' => 10.0
        ]);
        $search->setFacetWeights([
            RDF::RDF_TYPE                                            => [
                'https://vocabs.acdh.oeaw.ac.at/schema#TopCollection' => 10.0,
                'https://vocabs.acdh.oeaw.ac.at/schema#Collection'    => 5.0,
            ],
            'https://vocabs.acdh.oeaw.ac.at/schema#hasAvailableDate' => 'desc',
        ]);
        $search->setNamedEntityWeights([
            'https://vocabs.acdh.oeaw.ac.at/schema#hasAuthor' => 10,
        ]);
        $search->setNamedEntityFilter([
            'https://vocabs.acdh.oeaw.ac.at/schema#Organisation',
            'https://vocabs.acdh.oeaw.ac.at/schema#Person',
            'https://vocabs.acdh.oeaw.ac.at/schema#Place',
            'https://vocabs.acdh.oeaw.ac.at/schema#Project',
            'https://vocabs.acdh.oeaw.ac.at/schema#Publication',
        ]);
        $search->search('Schnitzler', 'en', true, true);
        $search->getSearchFacets();
        $cfg                     = new SearchConfig();
        $cfg->metadataMode       = 'resource';
        $cfg->resourceProperties = array_keys($context);
        $iter                    = $search->getSearchPage(1, 5, $cfg);

        $resources  = [];
        $totalCount = 0;
        foreach ($iter as $triple) {
            if ($triple->property === $schema->searchCount) {
                $totalCount = (int) $triple->value;
                continue;
            }
            $property = $context[$triple->property] ?? false;
            if ($property) {
                $id             = (string) $triple->id;
                $resources[$id] ??= (object)['id' => $triple->id];
                if (!empty($triple->lang)) {
                    $resources[$id]->{$property}[$triple->lang] = $triple->value;
                } else {
                    $resources[$id]->$property = $triple->value;
                }
            } else {
                $log->error("SKIPPED " . print_r($triple, true));
            }
        }
        $order = array_map(fn($x) => $x->matchOrder, $resources);
        array_multisort($order, $resources);
        $log->info(print_r($resources, true));
    }
}

//class MyLog extends \Psr\Log\AbstractLogger {
//
//    public function log($level, string | \Stringable $message,
//                        array $context = []): void {
//        $fh = fopen('php://stdout', 'w');
//        fwrite($fh, $message . "\n");
//        fclose($fh);
//    }
//}
