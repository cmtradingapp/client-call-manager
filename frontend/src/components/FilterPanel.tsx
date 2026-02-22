import React, { useEffect, useState } from 'react';

import { getCountries, getStatuses } from '../api/client';
import { useClientSearch } from '../hooks/useClientSearch';
import { useAppStore } from '../store/useAppStore';
import type { Country, SalesStatus } from '../types';

const LANGUAGES = [
  { code: 'EN', label: 'English' },
  { code: 'ES', label: 'Spanish' },
  { code: 'AR', label: 'Arabic' },
];

export function FilterPanel() {
  const { filters, setFilters, resetFilters, isSearching } = useAppStore();
  const { search } = useClientSearch();
  const [countries, setCountries] = useState<Country[]>([]);
  const [statuses, setStatuses] = useState<SalesStatus[]>([]);

  useEffect(() => {
    getCountries().then(setCountries).catch(() => {});
    getStatuses().then(setStatuses).catch(() => {});
  }, []);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    search();
  };

  return (
    <form onSubmit={handleSubmit} className="bg-white rounded-lg shadow p-6 space-y-4">
      <h2 className="text-lg font-semibold text-gray-800">Search Filters</h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {/* Date From */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Date From
          </label>
          <input
            type="date"
            value={filters.date_from ?? ''}
            onChange={(e) =>
              setFilters({ date_from: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        {/* Date To */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Date To
          </label>
          <input
            type="date"
            value={filters.date_to ?? ''}
            onChange={(e) =>
              setFilters({ date_to: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>

        {/* Sales Status */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Status
          </label>
          <select
            value={filters.sales_status ?? ''}
            onChange={(e) =>
              setFilters({ sales_status: e.target.value ? Number(e.target.value) : undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All Statuses</option>
            {statuses.map((s) => (
              <option key={s.id} value={s.id}>
                {s.value}
              </option>
            ))}
          </select>
        </div>

        {/* Country */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Country
          </label>
          <select
            value={filters.region ?? ''}
            onChange={(e) =>
              setFilters({ region: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All Countries</option>
            {countries.map((c) => (
              <option key={c.iso2code} value={c.iso2code}>
                {c.name}
              </option>
            ))}
          </select>
        </div>

        {/* Language */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Language
          </label>
          <select
            value={filters.language ?? ''}
            onChange={(e) =>
              setFilters({ language: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All Languages</option>
            {LANGUAGES.map((l) => (
              <option key={l.code} value={l.code}>
                {l.label}
              </option>
            ))}
          </select>
        </div>

        {/* FTD */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            FTD
          </label>
          <select
            value={filters.ftd ?? ''}
            onChange={(e) =>
              setFilters({ ftd: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All</option>
            <option value="yes">Yes</option>
            <option value="no">No</option>
          </select>
        </div>

        {/* Live */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Live
          </label>
          <select
            value={filters.live ?? ''}
            onChange={(e) =>
              setFilters({ live: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All</option>
            <option value="yes">Yes</option>
            <option value="no">No</option>
          </select>
        </div>

        {/* Client Potential */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Client Potential
          </label>
          <div className="flex gap-2">
            <select
              value={filters.sales_client_potential_op ?? 'eq'}
              onChange={(e) => setFilters({ sales_client_potential_op: e.target.value })}
              className="w-28 border border-gray-300 rounded-md px-2 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="eq">= Equal</option>
              <option value="gt">&gt; Greater</option>
              <option value="gte">≥ Greater =</option>
              <option value="lt">&lt; Less</option>
              <option value="lte">≤ Less =</option>
            </select>
            <input
              type="number"
              placeholder="e.g. 3"
              value={filters.sales_client_potential ?? ''}
              onChange={(e) =>
                setFilters({
                  sales_client_potential: e.target.value ? Number(e.target.value) : undefined,
                })
              }
              className="flex-1 border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        {/* Custom Field */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Search
          </label>
          <input
            type="text"
            placeholder="Search name or email…"
            value={filters.custom_field ?? ''}
            onChange={(e) =>
              setFilters({ custom_field: e.target.value || undefined })
            }
            className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
        </div>
      </div>

      <div className="flex gap-3 pt-2">
        <button
          type="submit"
          disabled={isSearching}
          className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {isSearching ? 'Searching…' : 'Search'}
        </button>
        <button
          type="button"
          onClick={resetFilters}
          className="px-4 py-2 bg-gray-100 text-gray-700 rounded-md text-sm font-medium hover:bg-gray-200 transition-colors"
        >
          Reset
        </button>
      </div>
    </form>
  );
}
