<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
