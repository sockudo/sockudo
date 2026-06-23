import { HomeLayout } from 'fumadocs-ui/layouts/home';
import { baseOptions } from '@/lib/layout.shared';

export default function Layout({ children }: LayoutProps<'/blog'>) {
  return <HomeLayout {...baseOptions()}>{children}</HomeLayout>;
}
