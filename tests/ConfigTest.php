<?php

/*
 * The MIT License
 *
 * Copyright 2022 Austrian Centre for Digital Humanities.
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

use acdhOeaw\arche\lib\Config;

/**
 * Description of ConfigTest
 *
 * @author zozlak
 */
class ConfigTest extends TestBase {

    public function testCopy(): void {
        $cfg1 = new Config(new \stdClass());
        $cfg2 = new Config($cfg1);

        // TODO - not sure if this is a preferred behavior but it works like that
        // since ever and it wasn't an issue yet
        /** @phpstan-ignore method.impossibleType */
        $this->assertNull($cfg1->uri);
        /** @phpstan-ignore method.impossibleType */
        $this->assertNull($cfg2->uri);
        $cfg1->uri = 'bar';
        $this->assertEquals('bar', $cfg1->uri);
        $this->assertEquals('bar', $cfg2->uri);
    }

    public function testSerialization(): void {
        $cfg      = new Config(new \stdClass());
        $cfg->uri = 'bar';

        $cfgProps = $cfg->asObject();
        $this->assertInstanceOf(\stdClass::class, $cfgProps);
        $this->assertEquals('bar', $cfgProps->uri ?? null);

        $cfgProps = $cfg->asArray();
        $this->assertEquals('bar', $cfgProps['uri'] ?? null);

        $yaml = $cfg->asYaml();
        $this->assertEquals(yaml_emit($cfgProps), $yaml);
    }
}
