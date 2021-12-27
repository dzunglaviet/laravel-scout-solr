<?php

namespace ScoutEngines\Solr;

use Illuminate\Support\ServiceProvider;
use Laravel\Scout\EngineManager;
use Solarium\Client;

class SolrProvider extends ServiceProvider
{
    /**
     * Bootstrap the application services.
     */
    public function boot()
    {
        app(EngineManager::class)->extend('solr', function ($app) {
            
            $config = [
                'endpoint' => [
                    'default' => [
                        'host' => config('scout.solr.host'),
                        'port' => config('scout.solr.port'),
                        'path' => config('scout.solr.path'),
                        'core' => config('scout.solr.core'),
                        'username' => config('scout.solr.username'),
                        'password' => config('scout.solr.password'),
                    ],
                ],
            ];

            return new SolrEngine($config);
        });
    }
}
