<?php

/*
 * The MIT License
 *
 * Copyright 2019 Austrian Centre for Digital Humanities.
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

namespace acdhOeaw\acdhRepoLib;

use GuzzleHttp\Psr7\Request;
use exception\RepoLibException;

/**
 * Simple container for a request binary payload
 *
 * @author zozlak
 */
class BinaryPayload {

    public $data;
    public $fileName;
    public $mimeType;

    public function __construct(?string $data = null, ?string $filePath = null,
                                ?string $mimeType = null) {
        if ($data !== null) {
            $this->createFromData($data, basename($filePath), $mimeType);
        } elseif ($filePath !== null) {
            $this->createFromFile($filePath, $mimeType);
        }
    }

    public function __destruct() {
        if (is_resource($this->data)) {
            fclose($this->data);
        }
    }

    public function attachTo(Request $request): Request {
        $headers = $request->getHeaders();
        if (!empty($this->fileName)) {
            $headers['Content-Disposition'] = 'attachment; filename="' . $this->fileName . '"';
        }
        if ($this->mimeType) {
            $headers['Content-Type'] = $this->mimeType;
        }
        return new Request($request->getMethod(), $request->getUri(), $headers, $this->data);
    }

    private function createFromData(string $data, ?string $fileName,
                                    ?string $mimeType): void {
        $this->data = $data;
        if (!empty($fileName)) {
            $this->fileName = $fileName;
            $this->mimeType = GuzzleHttp\Psr7\mimetype_from_filename(($fileName));
        }
        if (!empty($mimeType)) {
            $this->mimeType = $mimeType;
        }
    }

    private function createFromFile(string $path, ?string $mimeType): void {
        if (!file_exists($path)) {
            throw new RepoLibException('No such file');
        }

        $this->data     = fopen($path, 'rb');
        $this->fileName = basename($path);

        if (!empty($mimeType)) {
            $this->mimeType = $mimeType;
        } else {
            $this->mimeType = \GuzzleHttp\Psr7\mimetype_from_filename(basename($path));
            if ($this->mimeType === null) {
                $this->mimeType = @mime_content_type($this->$path);
            }
        }
    }

}
