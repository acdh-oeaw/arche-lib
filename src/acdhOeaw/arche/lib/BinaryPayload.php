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

namespace acdhOeaw\arche\lib;

use GuzzleHttp\Psr7\Request;
use acdhOeaw\arche\lib\exception\RepoLibException;

/**
 * Simple container for a request binary payload
 *
 * @author zozlak
 */
class BinaryPayload {

    const GUZZLE_PSR7_V1_MIME_FUNC = '\GuzzleHttp\Psr7\mimetype_from_filename';
    const GUZZLE_PSR7_V2_MIME_FUNC = '\GuzzleHttp\Psr7\MimeType::fromFilename';

    static public function guzzleMimetype(string $fileName): ?string {
        $f = function_exists(self::GUZZLE_PSR7_V1_MIME_FUNC) ? self::GUZZLE_PSR7_V1_MIME_FUNC : self::GUZZLE_PSR7_V2_MIME_FUNC;
        return $f($fileName);
    }

    /**
     * A stream object or the data as a string.
     * 
     * @var mixed
     */
    public $data;

    /**
     * Name of the file data come from.
     * 
     * @var string
     */
    public $fileName;

    /**
     * Mime type of the data.
     * 
     * @var string
     */
    public $mimeType;

    /**
     * Creates a binary payload object.
     * 
     * @param string|null $data data as a string (pass null for creating a payload 
     *   from a file)
     * @param string|null $filePath path to a file (when creating a payload from a file)
     *   or a filename to be stored in the metadata (when creating a payload from string)
     * @param string|null $mimeType mime type of the data; if not provided it will be guessed 
     *   from the file name and/or data content
     */
    public function __construct(?string $data = null, ?string $filePath = null,
                                ?string $mimeType = null) {
        if ($data !== null) {
            $this->createFromData($data, basename((string) $filePath), $mimeType);
        } elseif ($filePath !== null) {
            $this->createFromFile($filePath, $mimeType);
        }
    }

    /**
     * Makes sure all data streams are closed.
     */
    public function __destruct() {
        if (is_resource($this->data)) {
            fclose($this->data);
        }
    }

    /**
     * Attaches the data to a given HTTP request.
     * 
     * @param Request $request PSR-7 request
     * @return Request
     */
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

    /**
     * Initializes the object from the data in string.
     * 
     * @param string $data data
     * @param string|null $fileName file name to be passed to the repository
     * @param string|null $mimeType mime type
     * @return void
     */
    private function createFromData(string $data, ?string $fileName,
                                    ?string $mimeType): void {
        $this->data = $data;
        if (!empty($fileName)) {
            $this->fileName = $fileName;
            $this->mimeType = self::guzzleMimetype($this->fileName) ?? '';
        }
        if (!empty($mimeType)) {
            $this->mimeType = $mimeType;
        }
    }

    /**
     * Initializes the object from a file.
     * 
     * @param string $path file path
     * @param string|null $mimeType mime type 
     *   (if not provided it will be guessed based on the file name and/or file content)
     * @return void
     * @throws RepoLibException
     */
    private function createFromFile(string $path, ?string $mimeType): void {
        if (!file_exists($path)) {
            throw new RepoLibException('No such file');
        }

        $this->data     = fopen($path, 'rb');
        $this->fileName = basename($path);

        if (!empty($mimeType)) {
            $this->mimeType = $mimeType;
        } else {
            $this->mimeType = self::guzzleMimetype(basename($path)) ?? '';
            if ($this->mimeType === null) {
                $this->mimeType = (string) @mime_content_type($path);
            }
        }
    }
}
